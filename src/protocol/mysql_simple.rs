use bytes::{BufMut, BytesMut};
use sha1::{Digest, Sha1};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, info};

use crate::YamlBaseError;
use crate::config::Config;
use crate::database::Storage;
use crate::protocol::mysql_caching_sha2::{CACHING_SHA2_PLUGIN_NAME, CachingSha2Auth};
use crate::sql::{QueryExecutor, parse_sql};

// MySQL Protocol Constants
const PROTOCOL_VERSION: u8 = 10;
const SERVER_VERSION: &str = "8.0.35-yamlbase";
const AUTH_PLUGIN_NAME: &str = "mysql_native_password";

// Command bytes
const COM_QUIT: u8 = 0x01;
const COM_INIT_DB: u8 = 0x02;
const COM_QUERY: u8 = 0x03;
const COM_PING: u8 = 0x0e;

// Capability flags
const CLIENT_LONG_PASSWORD: u32 = 0x00000001;
const CLIENT_FOUND_ROWS: u32 = 0x00000002;
const CLIENT_LONG_FLAG: u32 = 0x00000004;
const CLIENT_CONNECT_WITH_DB: u32 = 0x00000008;
const CLIENT_PROTOCOL_41: u32 = 0x00000200;
const CLIENT_SECURE_CONNECTION: u32 = 0x00008000;
const CLIENT_PLUGIN_AUTH: u32 = 0x00080000;
const _CLIENT_DEPRECATE_EOF: u32 = 0x01000000;

// Column types
const MYSQL_TYPE_VAR_STRING: u8 = 253;

// Status flags
const SERVER_STATUS_AUTOCOMMIT: u16 = 0x0002;

pub struct MySqlProtocol {
    config: Arc<Config>,
    executor: QueryExecutor,
    _database_name: String,
}

struct ConnectionState {
    sequence_id: u8,
    _capabilities: u32,
    auth_data: Vec<u8>,
    client_auth_plugin: Option<String>,
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self {
            sequence_id: 0,
            _capabilities: 0,
            auth_data: generate_auth_data(),
            client_auth_plugin: None,
        }
    }
}

impl MySqlProtocol {
    pub async fn new(config: Arc<Config>, storage: Arc<Storage>) -> crate::Result<Self> {
        let executor = QueryExecutor::new(storage).await?;
        Ok(Self {
            config,
            executor,
            _database_name: String::new(), // Will be set later if needed
        })
    }

    pub async fn handle_connection(&self, mut stream: TcpStream) -> crate::Result<()> {
        info!("New MySQL connection");

        let mut state = ConnectionState::default();

        // Send initial handshake
        self.send_handshake(&mut stream, &mut state).await?;

        // Read handshake response
        let response_packet = self.read_packet(&mut stream, &mut state).await?;
        let (username, auth_response, _database, client_plugin) =
            self.parse_handshake_response(&response_packet)?;
        state.client_auth_plugin = client_plugin;

        // Simple authentication check
        debug!(
            "Authentication check - username: {}, expected: {}",
            username, self.config.username
        );
        if username != self.config.username {
            debug!("Username mismatch");
            self.send_error(&mut stream, &mut state, 1045, "28000", "Access denied")
                .await?;
            return Ok(());
        }

        // Verify password
        let expected = compute_auth_response(&self.config.password, &state.auth_data);
        debug!(
            "Password check - auth_response len: {}, expected len: {}, config password: {}",
            auth_response.len(),
            expected.len(),
            self.config.password
        );

        // Check if client requested caching_sha2_password
        let client_wants_caching = state
            .client_auth_plugin
            .as_ref()
            .map(|p| p == CACHING_SHA2_PLUGIN_NAME)
            .unwrap_or(false);

        if client_wants_caching || auth_response.is_empty() {
            // Switch to caching_sha2_password
            debug!("Client requested caching_sha2_password or sent empty auth");

            // Generate new auth data for caching_sha2
            let caching_auth_data = generate_auth_data();
            let caching_auth = CachingSha2Auth::new(caching_auth_data.clone());

            // Send auth switch request
            caching_auth
                .send_auth_switch_request(&mut stream, &mut state.sequence_id)
                .await?;

            // Read client's response to auth switch
            let auth_switch_response = self.read_packet(&mut stream, &mut state).await?;

            // Authenticate using caching_sha2_password
            let auth_success = caching_auth
                .authenticate(
                    &mut stream,
                    &mut state.sequence_id,
                    &username,
                    "", // password will be sent in clear text
                    &self.config.username,
                    &self.config.password,
                    auth_switch_response,
                )
                .await?;

            if !auth_success {
                self.send_error(&mut stream, &mut state, 1045, "28000", "Access denied")
                    .await?;
                return Ok(());
            }
        } else {
            // Use mysql_native_password authentication
            if auth_response != expected {
                debug!(
                    "Password mismatch - expected: {:?}, got: {:?}",
                    expected, auth_response
                );
                self.send_error(&mut stream, &mut state, 1045, "28000", "Access denied")
                    .await?;
                return Ok(());
            }
        }

        // Send OK packet
        self.send_ok(&mut stream, &mut state, 0, 0).await?;
        info!("MySQL authentication successful, entering command loop");

        // Main command loop
        loop {
            let packet = match self.read_packet(&mut stream, &mut state).await {
                Ok(p) => p,
                Err(_) => break,
            };

            if packet.is_empty() {
                continue;
            }

            let command = packet[0];
            match command {
                COM_QUERY => {
                    let query = std::str::from_utf8(&packet[1..]).map_err(|_| {
                        YamlBaseError::Protocol("Invalid UTF-8 in query".to_string())
                    })?;
                    self.handle_query(&mut stream, &mut state, query).await?;
                }
                COM_QUIT => {
                    info!("Client disconnected");
                    break;
                }
                COM_PING => {
                    self.send_ok(&mut stream, &mut state, 0, 0).await?;
                }
                COM_INIT_DB => {
                    let _db_name = std::str::from_utf8(&packet[1..]).map_err(|_| {
                        YamlBaseError::Protocol("Invalid UTF-8 in database name".to_string())
                    })?;
                    self.send_ok(&mut stream, &mut state, 0, 0).await?;
                }
                _ => {
                    debug!("Unhandled command: 0x{:02x}", command);
                    self.send_error(&mut stream, &mut state, 1047, "08S01", "Unknown command")
                        .await?;
                }
            }
        }

        Ok(())
    }

    async fn send_handshake(
        &self,
        stream: &mut TcpStream,
        state: &mut ConnectionState,
    ) -> crate::Result<()> {
        let mut packet = BytesMut::new();

        // Protocol version
        packet.put_u8(PROTOCOL_VERSION);

        // Server version
        packet.put_slice(SERVER_VERSION.as_bytes());
        packet.put_u8(0);

        // Connection ID
        packet.put_u32_le(1);

        // Auth data part 1 (8 bytes)
        packet.put_slice(&state.auth_data[..8]);

        // Filler
        packet.put_u8(0);

        // Capability flags (lower 2 bytes)
        let capabilities = CLIENT_LONG_PASSWORD
            | CLIENT_FOUND_ROWS
            | CLIENT_LONG_FLAG
            | CLIENT_CONNECT_WITH_DB
            | CLIENT_PROTOCOL_41
            | CLIENT_SECURE_CONNECTION
            | CLIENT_PLUGIN_AUTH;
        packet.put_u16_le((capabilities & 0xFFFF) as u16);

        // Character set (utf8mb4)
        packet.put_u8(33);

        // Status flags
        packet.put_u16_le(SERVER_STATUS_AUTOCOMMIT);

        // Capability flags (upper 2 bytes)
        packet.put_u16_le(((capabilities >> 16) & 0xFFFF) as u16);

        // Length of auth plugin data
        packet.put_u8(21);

        // Reserved
        packet.put_slice(&[0; 10]);

        // Auth data part 2 (12 bytes)
        packet.put_slice(&state.auth_data[8..20]);
        packet.put_u8(0);

        // Auth plugin name
        packet.put_slice(AUTH_PLUGIN_NAME.as_bytes());
        packet.put_u8(0);

        self.write_packet(stream, state, &packet).await?;
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    fn parse_handshake_response(
        &self,
        packet: &[u8],
    ) -> crate::Result<(String, Vec<u8>, Option<String>, Option<String>)> {
        debug!("Parsing handshake response, packet len: {}", packet.len());
        let mut pos = 0;

        // Parse client capabilities (4 bytes)
        let client_flags = u32::from_le_bytes([
            packet[pos],
            packet[pos + 1],
            packet[pos + 2],
            packet[pos + 3],
        ]);
        debug!("Client capabilities: 0x{:08x}", client_flags);
        pos += 4;

        // Skip max packet size (4 bytes)
        pos += 4;

        // Skip character set (1 byte)
        pos += 1;

        // Skip reserved (23 bytes)
        pos += 23;

        // Username (null-terminated)
        let username_end = packet[pos..]
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| YamlBaseError::Protocol("Invalid handshake response".to_string()))?;
        let username = std::str::from_utf8(&packet[pos..pos + username_end])
            .map_err(|_| YamlBaseError::Protocol("Invalid UTF-8 in username".to_string()))?
            .to_string();
        debug!("Username: {}", username);
        pos += username_end + 1;

        // Auth response length
        let auth_len = packet[pos] as usize;
        debug!(
            "Auth response length byte: {}, interpreted as: {}",
            packet[pos], auth_len
        );
        pos += 1;

        // Auth response
        let auth_response = if auth_len > 0 && pos + auth_len <= packet.len() {
            packet[pos..pos + auth_len].to_vec()
        } else {
            debug!("Auth response empty or invalid length");
            Vec::new()
        };
        pos += auth_len;

        // Database (optional, null-terminated)
        let database = if pos < packet.len() {
            let db_end = packet[pos..]
                .iter()
                .position(|&b| b == 0)
                .unwrap_or(packet.len() - pos);
            if db_end > 0 {
                Some(
                    std::str::from_utf8(&packet[pos..pos + db_end])
                        .map_err(|_| {
                            YamlBaseError::Protocol("Invalid UTF-8 in database".to_string())
                        })?
                        .to_string(),
                )
            } else {
                None
            }
        } else {
            None
        };

        // Try to read auth plugin name if present
        let auth_plugin = if pos < packet.len() {
            // Skip to auth plugin name (may have client attributes first)
            // For simplicity, we'll just check if there's more data
            let plugin_end = packet[pos..]
                .iter()
                .position(|&b| b == 0)
                .unwrap_or(packet.len() - pos);
            if plugin_end > 0 {
                Some(
                    std::str::from_utf8(&packet[pos..pos + plugin_end])
                        .map_err(|_| {
                            YamlBaseError::Protocol("Invalid UTF-8 in auth plugin".to_string())
                        })?
                        .to_string(),
                )
            } else {
                None
            }
        } else {
            None
        };

        debug!("Client auth plugin: {:?}", auth_plugin);

        Ok((username, auth_response, database, auth_plugin))
    }

    async fn handle_query(
        &self,
        stream: &mut TcpStream,
        state: &mut ConnectionState,
        query: &str,
    ) -> crate::Result<()> {
        let query_trimmed = query.trim();
        let query_upper = query_trimmed.to_uppercase();

        // Handle empty queries
        if query_trimmed.is_empty() {
            debug!("Empty query received");
            self.send_error(stream, state, 1064, "42000", "Syntax error: Empty query")
                .await?;
            return Ok(());
        }

        // Handle queries with system variables by preprocessing them
        let mut processed_query = if query_trimmed.contains("@@") {
            self.preprocess_system_variables(query_trimmed)
        } else {
            query_trimmed.to_string()
        };

        // Convert MySQL backticks - just remove them since our parser handles unquoted identifiers
        if processed_query.contains('`') {
            processed_query = processed_query.replace('`', "");
            debug!("Removed backticks: {}", processed_query);
        }

        // Handle SET NAMES command (ignore it - we always use UTF-8)
        if query_upper.starts_with("SET NAMES") || query_upper.starts_with("SET CHARACTER SET") {
            debug!("Ignoring SET NAMES/CHARACTER SET command: {}", query);
            return self.send_ok(stream, state, 0, 0).await;
        }

        // Handle other SET commands that MySQL clients might send
        if query_upper.starts_with("SET ") {
            debug!("Ignoring SET command: {}", query);
            return self.send_ok(stream, state, 0, 0).await;
        }

        // Parse SQL
        let statements = match parse_sql(&processed_query) {
            Ok(stmts) => stmts,
            Err(e) => {
                self.send_error(
                    stream,
                    state,
                    1064,
                    "42000",
                    &format!("Syntax error: {}", e),
                )
                .await?;
                return Ok(());
            }
        };

        for statement in statements {
            debug!("Executing statement: {:?}", statement);
            match self.executor.execute(&statement).await {
                Ok(result) => {
                    debug!(
                        "Query executed successfully. Result: {} columns, {} rows",
                        result.columns.len(),
                        result.rows.len()
                    );
                    self.send_query_result(stream, state, &result).await?;
                }
                Err(e) => {
                    debug!("Query execution error: {}", e);
                    self.send_error(stream, state, 1146, "42S02", &e.to_string())
                        .await?;
                }
            }
        }

        Ok(())
    }

    fn preprocess_system_variables(&self, query: &str) -> String {
        use once_cell::sync::Lazy;
        use regex::Regex;

        // Only preprocess SELECT queries that contain system variables
        let query_upper = query.to_uppercase();
        if !query_upper.starts_with("SELECT") || !query.contains("@@") {
            return query.to_string();
        }

        static VERSION_RE: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"@@(?:(?:global|GLOBAL|Global|session|SESSION|Session)\.)?(?:version|VERSION|Version)\b").unwrap()
        });

        static VERSION_COMMENT_RE: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"@@(?:(?:global|GLOBAL|Global|session|SESSION|Session)\.)?(?:version_comment|VERSION_COMMENT|Version_Comment)\b").unwrap()
        });

        static MAX_ALLOWED_PACKET_RE: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"@@(?:(?:global|GLOBAL|Global|session|SESSION|Session)\.)?(?:max_allowed_packet|MAX_ALLOWED_PACKET|Max_Allowed_Packet)\b").unwrap()
        });

        static SYSTEM_VAR_RE: Lazy<Regex> = Lazy::new(|| {
            Regex::new(r"@@(?:(?:global|GLOBAL|Global|session|SESSION|Session)\.)?([a-zA-Z_][a-zA-Z0-9_]*)\b").unwrap()
        });

        let mut result = query.to_string();

        // First handle @@version specifically
        result = VERSION_RE
            .replace_all(&result, "'8.0.35-yamlbase'")
            .to_string();

        // Handle @@version_comment
        result = VERSION_COMMENT_RE.replace_all(&result, "'1'").to_string();

        // Handle @@max_allowed_packet - MySQL default is 64MB (67108864 bytes)
        result = MAX_ALLOWED_PACKET_RE
            .replace_all(&result, "67108864")
            .to_string();

        // Check if we already replaced all instances
        if !result.contains("@@") {
            debug!("Preprocessed query: {} -> {}", query, result);
            return result;
        }

        // Replace remaining system variables with '1'
        result = SYSTEM_VAR_RE.replace_all(&result, "'1'").to_string();

        debug!("Preprocessed query: {} -> {}", query, result);
        result
    }

    async fn send_query_result(
        &self,
        stream: &mut TcpStream,
        state: &mut ConnectionState,
        result: &crate::sql::executor::QueryResult,
    ) -> crate::Result<()> {
        debug!(
            "Sending query result with {} columns and {} rows",
            result.columns.len(),
            result.rows.len()
        );

        // Convert to string representation
        let columns: Vec<&str> = result.columns.iter().map(|s| s.as_str()).collect();
        debug!("Columns: {:?}", columns);

        let rows: Vec<Vec<String>> = result
            .rows
            .iter()
            .map(|row| row.iter().map(|val| val.to_string()).collect())
            .collect();
        debug!("Converted {} rows to strings", rows.len());

        let string_rows: Vec<Vec<&str>> = rows
            .iter()
            .map(|row| row.iter().map(|s| s.as_str()).collect())
            .collect();

        debug!("Calling send_simple_result_set");
        self.send_simple_result_set(stream, state, &columns, &string_rows)
            .await
    }

    async fn send_simple_result_set(
        &self,
        stream: &mut TcpStream,
        state: &mut ConnectionState,
        columns: &[&str],
        rows: &[Vec<&str>],
    ) -> crate::Result<()> {
        debug!(
            "send_simple_result_set: {} columns, {} rows",
            columns.len(),
            rows.len()
        );

        // Column count
        let mut packet = BytesMut::new();
        packet.put_u8(columns.len() as u8);
        debug!("Writing column count packet");
        self.write_packet(stream, state, &packet).await?;

        // Column definitions
        debug!("Writing {} column definitions", columns.len());
        for (idx, column) in columns.iter().enumerate() {
            debug!("Writing column definition {}: {}", idx, column);
            let mut col_packet = BytesMut::new();

            // Catalog (def)
            col_packet.put_u8(3);
            col_packet.put_slice(b"def");

            // Schema
            col_packet.put_u8(0);

            // Table
            col_packet.put_u8(0);

            // Original table
            col_packet.put_u8(0);

            // Column name
            col_packet.put_u8(column.len() as u8);
            col_packet.put_slice(column.as_bytes());

            // Original column name
            col_packet.put_u8(column.len() as u8);
            col_packet.put_slice(column.as_bytes());

            // Length of fixed fields (0x0c)
            col_packet.put_u8(0x0c);

            // Character set (utf8mb4)
            col_packet.put_u16_le(33);

            // Column length
            col_packet.put_u32_le(255);

            // Column type (VAR_STRING)
            col_packet.put_u8(MYSQL_TYPE_VAR_STRING);

            // Flags
            col_packet.put_u16_le(0);

            // Decimals
            col_packet.put_u8(0);

            // Filler
            col_packet.put_u16_le(0);

            self.write_packet(stream, state, &col_packet).await?;
        }

        // Send EOF packet after column definitions (for clients that don't support CLIENT_DEPRECATE_EOF)
        debug!("Sending EOF packet after column definitions");
        let mut eof_packet = BytesMut::new();
        eof_packet.put_u8(0xfe); // EOF marker
        eof_packet.put_u16_le(0); // warnings
        eof_packet.put_u16_le(SERVER_STATUS_AUTOCOMMIT); // status flags
        self.write_packet(stream, state, &eof_packet).await?;

        // Send rows
        debug!("Sending {} rows", rows.len());
        for (idx, row) in rows.iter().enumerate() {
            debug!("Sending row {} with {} values", idx, row.len());
            let mut row_packet = BytesMut::new();
            for (col_idx, value) in row.iter().enumerate() {
                if *value == "NULL" {
                    debug!("  Column {}: NULL", col_idx);
                    row_packet.put_u8(0xfb); // NULL value
                } else {
                    let bytes = value.as_bytes();
                    debug!("  Column {}: '{}' ({} bytes)", col_idx, value, bytes.len());
                    // MySQL uses length-encoded strings for result rows
                    if bytes.len() < 251 {
                        row_packet.put_u8(bytes.len() as u8);
                    } else if bytes.len() < 65536 {
                        row_packet.put_u8(0xfc);
                        row_packet.put_u16_le(bytes.len() as u16);
                    } else if bytes.len() < 16777216 {
                        row_packet.put_u8(0xfd);
                        row_packet.put_u8((bytes.len() & 0xff) as u8);
                        row_packet.put_u8(((bytes.len() >> 8) & 0xff) as u8);
                        row_packet.put_u8(((bytes.len() >> 16) & 0xff) as u8);
                    } else {
                        row_packet.put_u8(0xfe);
                        row_packet.put_u64_le(bytes.len() as u64);
                    }
                    row_packet.put_slice(bytes);
                }
            }
            debug!("Row packet size: {} bytes", row_packet.len());
            self.write_packet(stream, state, &row_packet).await?;
        }

        // Send EOF packet after rows
        debug!("Sending final EOF packet");
        let mut eof_packet = BytesMut::new();
        eof_packet.put_u8(0xfe); // EOF marker
        eof_packet.put_u16_le(0); // warnings
        eof_packet.put_u16_le(SERVER_STATUS_AUTOCOMMIT); // status flags
        self.write_packet(stream, state, &eof_packet).await
    }

    async fn send_ok(
        &self,
        stream: &mut TcpStream,
        state: &mut ConnectionState,
        affected_rows: u64,
        _info: u64,
    ) -> crate::Result<()> {
        let mut packet = BytesMut::new();

        // OK packet header
        packet.put_u8(0x00);

        // Affected rows
        put_lenenc_int(&mut packet, affected_rows);

        // Last insert ID
        put_lenenc_int(&mut packet, 0);

        // Status flags
        packet.put_u16_le(SERVER_STATUS_AUTOCOMMIT);

        // Warnings
        packet.put_u16_le(0);

        self.write_packet(stream, state, &packet).await
    }

    async fn send_error(
        &self,
        stream: &mut TcpStream,
        state: &mut ConnectionState,
        error_code: u16,
        sql_state: &str,
        message: &str,
    ) -> crate::Result<()> {
        let mut packet = BytesMut::new();

        // Error packet header
        packet.put_u8(0xff);

        // Error code
        packet.put_u16_le(error_code);

        // SQL state marker
        packet.put_u8(b'#');

        // SQL state
        packet.put_slice(sql_state.as_bytes());

        // Error message
        packet.put_slice(message.as_bytes());

        self.write_packet(stream, state, &packet).await
    }

    async fn write_packet(
        &self,
        stream: &mut TcpStream,
        state: &mut ConnectionState,
        payload: &[u8],
    ) -> crate::Result<()> {
        let mut packet = BytesMut::with_capacity(4 + payload.len());

        // Length (3 bytes)
        packet.put_u8((payload.len() & 0xff) as u8);
        packet.put_u8(((payload.len() >> 8) & 0xff) as u8);
        packet.put_u8(((payload.len() >> 16) & 0xff) as u8);

        // Sequence ID
        packet.put_u8(state.sequence_id);

        debug!(
            "Writing packet: len={}, seq={}, first_bytes={:?}",
            payload.len(),
            state.sequence_id,
            &payload[..std::cmp::min(20, payload.len())]
        );

        state.sequence_id = state.sequence_id.wrapping_add(1);

        // Payload
        packet.put_slice(payload);

        stream.write_all(&packet).await?;
        stream.flush().await?;

        Ok(())
    }

    async fn read_packet(
        &self,
        stream: &mut TcpStream,
        state: &mut ConnectionState,
    ) -> crate::Result<Vec<u8>> {
        let mut header = [0u8; 4];
        stream.read_exact(&mut header).await?;

        let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
        state.sequence_id = header[3].wrapping_add(1);

        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await?;

        Ok(payload)
    }
}

fn generate_auth_data() -> Vec<u8> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let mut auth_data = vec![0u8; 20];
    rng.fill(&mut auth_data[..]);
    auth_data
}

fn compute_auth_response(password: &str, auth_data: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }

    // SHA1(password)
    let mut hasher = Sha1::new();
    hasher.update(password.as_bytes());
    let stage1 = hasher.finalize();

    // SHA1(SHA1(password))
    let mut hasher = Sha1::new();
    hasher.update(stage1);
    let stage2 = hasher.finalize();

    // SHA1(auth_data + SHA1(SHA1(password)))
    let mut hasher = Sha1::new();
    hasher.update(auth_data);
    hasher.update(stage2);
    let result = hasher.finalize();

    // XOR with SHA1(password)
    stage1
        .iter()
        .zip(result.iter())
        .map(|(a, b)| a ^ b)
        .collect()
}

fn put_lenenc_int(buf: &mut BytesMut, value: u64) {
    if value < 251 {
        buf.put_u8(value as u8);
    } else if value < 65536 {
        buf.put_u8(0xfc);
        buf.put_u16_le(value as u16);
    } else if value < 16777216 {
        buf.put_u8(0xfd);
        buf.put_u8((value & 0xff) as u8);
        buf.put_u8(((value >> 8) & 0xff) as u8);
        buf.put_u8(((value >> 16) & 0xff) as u8);
    } else {
        buf.put_u8(0xfe);
        buf.put_u64_le(value);
    }
}
