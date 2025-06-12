use bytes::{BufMut, BytesMut};
use sha1::{Digest, Sha1};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, info};

use crate::config::Config;
use crate::database::Database;
use crate::sql::{parse_sql, QueryExecutor};
use crate::YamlBaseError;

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
const CLIENT_DEPRECATE_EOF: u32 = 0x01000000;

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
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self {
            sequence_id: 0,
            _capabilities: 0,
            auth_data: generate_auth_data(),
        }
    }
}

impl MySqlProtocol {
    pub fn new(config: Arc<Config>, database: Arc<tokio::sync::RwLock<Database>>) -> Self {
        Self {
            config,
            executor: QueryExecutor::new(database),
            _database_name: String::new(), // Will be set later if needed
        }
    }

    pub async fn handle_connection(&self, mut stream: TcpStream) -> crate::Result<()> {
        info!("New MySQL connection");

        let mut state = ConnectionState::default();

        // Send initial handshake
        self.send_handshake(&mut stream, &mut state).await?;

        // Read handshake response
        let response_packet = self.read_packet(&mut stream, &mut state).await?;
        let (username, auth_response, _database) =
            self.parse_handshake_response(&response_packet)?;

        // Simple authentication check
        if username != self.config.username {
            self.send_error(&mut stream, &mut state, 1045, "28000", "Access denied")
                .await?;
            return Ok(());
        }

        // Verify password
        let expected = compute_auth_response(&self.config.password, &state.auth_data);
        if auth_response != expected {
            self.send_error(&mut stream, &mut state, 1045, "28000", "Access denied")
                .await?;
            return Ok(());
        }

        // Send OK packet
        self.send_ok(&mut stream, &mut state, 0, 0).await?;

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
            | CLIENT_PLUGIN_AUTH
            | CLIENT_DEPRECATE_EOF;
        packet.put_u16_le((capabilities & 0xFFFF) as u16);

        // Character set (utf8mb4)
        packet.put_u8(255);

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

    fn parse_handshake_response(
        &self,
        packet: &[u8],
    ) -> crate::Result<(String, Vec<u8>, Option<String>)> {
        let mut pos = 0;

        // Skip client capabilities (4 bytes)
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
        pos += username_end + 1;

        // Auth response length
        let auth_len = packet[pos] as usize;
        pos += 1;

        // Auth response
        let auth_response = packet[pos..pos + auth_len].to_vec();
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

        Ok((username, auth_response, database))
    }

    async fn handle_query(
        &self,
        stream: &mut TcpStream,
        state: &mut ConnectionState,
        query: &str,
    ) -> crate::Result<()> {
        debug!("Executing query: {}", query);

        // Handle special MySQL queries
        if query.trim().to_uppercase().starts_with("SELECT @@") {
            return self.handle_system_var_query(stream, state, query).await;
        }

        // Parse SQL
        let statements = match parse_sql(query) {
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
            match self.executor.execute(&statement).await {
                Ok(result) => {
                    self.send_query_result(stream, state, &result).await?;
                }
                Err(e) => {
                    self.send_error(stream, state, 1146, "42S02", &e.to_string())
                        .await?;
                }
            }
        }

        Ok(())
    }

    async fn handle_system_var_query(
        &self,
        stream: &mut TcpStream,
        state: &mut ConnectionState,
        query: &str,
    ) -> crate::Result<()> {
        let value = if query.contains("version") {
            SERVER_VERSION
        } else {
            "1"
        };

        // Send simple result set with one column and one row
        let columns = vec!["@@version"];
        let rows = vec![vec![value]];

        self.send_simple_result_set(stream, state, &columns, &rows)
            .await
    }

    async fn send_query_result(
        &self,
        stream: &mut TcpStream,
        state: &mut ConnectionState,
        result: &crate::sql::executor::QueryResult,
    ) -> crate::Result<()> {
        // Convert to string representation
        let columns: Vec<&str> = result.columns.iter().map(|s| s.as_str()).collect();
        let rows: Vec<Vec<String>> = result
            .rows
            .iter()
            .map(|row| row.iter().map(|val| val.to_string()).collect())
            .collect();

        let string_rows: Vec<Vec<&str>> = rows
            .iter()
            .map(|row| row.iter().map(|s| s.as_str()).collect())
            .collect();

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
        // Column count
        let mut packet = BytesMut::new();
        packet.put_u8(columns.len() as u8);
        self.write_packet(stream, state, &packet).await?;

        // Column definitions
        for column in columns {
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
            col_packet.put_u16_le(255);

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

        // Send rows
        for row in rows {
            let mut row_packet = BytesMut::new();
            for value in row {
                if *value == "NULL" {
                    row_packet.put_u8(0xfb); // NULL value
                } else {
                    let bytes = value.as_bytes();
                    if bytes.len() < 251 {
                        row_packet.put_u8(bytes.len() as u8);
                    } else {
                        row_packet.put_u8(0xfc);
                        row_packet.put_u16_le(bytes.len() as u16);
                    }
                    row_packet.put_slice(bytes);
                }
            }
            self.write_packet(stream, state, &row_packet).await?;
        }

        // Send OK packet (EOF replacement in CLIENT_DEPRECATE_EOF mode)
        self.send_ok(stream, state, 0, rows.len() as u64).await
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
    hasher.update(&stage1);
    let stage2 = hasher.finalize();

    // SHA1(auth_data + SHA1(SHA1(password)))
    let mut hasher = Sha1::new();
    hasher.update(auth_data);
    hasher.update(&stage2);
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
