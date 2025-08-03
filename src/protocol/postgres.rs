use bytes::{Buf, BufMut, BytesMut};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, info};

use crate::YamlBaseError;
use crate::config::Config;
use crate::database::{Storage, Value};
use crate::protocol::postgres_extended::ExtendedProtocol;
use crate::sql::{QueryExecutor, parse_sql};

pub struct PostgresProtocol {
    config: Arc<Config>,
    executor: QueryExecutor,
    _database_name: String,
    extended_protocol: ExtendedProtocol,
}

#[derive(Debug, Default)]
struct ConnectionState {
    authenticated: bool,
    username: Option<String>,
    database: Option<String>,
    parameters: HashMap<String, String>,
}

impl PostgresProtocol {
    pub async fn new(config: Arc<Config>, storage: Arc<Storage>) -> crate::Result<Self> {
        let executor = QueryExecutor::new(storage).await?;
        Ok(Self {
            config,
            executor,
            _database_name: String::new(), // Will be set later if needed
            extended_protocol: ExtendedProtocol::new(),
        })
    }

    pub async fn handle_connection(&mut self, mut stream: TcpStream) -> crate::Result<()> {
        info!("New PostgreSQL connection");

        let mut buffer = BytesMut::with_capacity(4096);
        let mut state = ConnectionState::default();

        // Read startup message
        self.read_startup_message(&mut stream, &mut buffer, &mut state)
            .await?;

        // Main message loop
        loop {
            // Read more data if buffer is empty
            if buffer.is_empty() && stream.read_buf(&mut buffer).await? == 0 {
                info!("Client disconnected");
                break;
            }

            // Check if we have enough data for a message header
            if buffer.len() < 5 {
                // Read more data
                if stream.read_buf(&mut buffer).await? == 0 {
                    info!("Client disconnected");
                    break;
                }
                continue;
            }

            let msg_type = buffer[0];
            let length = u32::from_be_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]) as usize;

            // Check if we have the complete message
            if buffer.len() < length + 1 {
                // Read more data
                if stream.read_buf(&mut buffer).await? == 0 {
                    return Ok(());
                }
                continue;
            }

            // Process message
            match msg_type {
                b'Q' => {
                    // Simple query
                    let query = self.parse_query(&buffer[5..length + 1])?;
                    self.handle_query(&mut stream, &query).await?;
                }
                b'P' => {
                    // Parse (extended query protocol)
                    self.extended_protocol
                        .handle_parse(&mut stream, &buffer[5..length + 1])
                        .await?;
                }
                b'B' => {
                    // Bind (extended query protocol)
                    self.extended_protocol
                        .handle_bind(&mut stream, &buffer[5..length + 1])
                        .await?;
                }
                b'D' => {
                    // Describe (extended query protocol)
                    self.extended_protocol
                        .handle_describe(&mut stream, &buffer[5..length + 1], &self.executor)
                        .await?;
                }
                b'E' => {
                    // Execute (extended query protocol)
                    self.extended_protocol
                        .handle_execute(&mut stream, &buffer[5..length + 1], &self.executor)
                        .await?;
                }
                b'S' => {
                    // Sync (extended query protocol)
                    self.extended_protocol.handle_sync(&mut stream).await?;
                }
                b'C' => {
                    // Close (extended query protocol)
                    let close_type = buffer[5];
                    let name_end = buffer[6..length + 1]
                        .iter()
                        .position(|&b| b == 0)
                        .unwrap_or(length - 5);
                    let name = std::str::from_utf8(&buffer[6..6 + name_end]).map_err(|_| {
                        YamlBaseError::Protocol("Invalid UTF-8 in close name".to_string())
                    })?;

                    if close_type == b'S' {
                        self.extended_protocol.close_statement(name);
                    } else if close_type == b'P' {
                        self.extended_protocol.close_portal(name);
                    }

                    // Send CloseComplete
                    let mut close_buf = BytesMut::new();
                    close_buf.put_u8(b'3');
                    close_buf.put_u32(4);
                    stream.write_all(&close_buf).await?;
                }
                b'X' => {
                    // Terminate
                    info!("Client requested termination");
                    break;
                }
                _ => {
                    debug!("Unhandled message type: {}", msg_type as char);
                    self.send_error(&mut stream, "XX000", "Unsupported operation")
                        .await?;
                }
            }

            // Remove the processed message from the buffer
            buffer.advance(length + 1);
        }

        Ok(())
    }

    async fn read_startup_message(
        &self,
        stream: &mut TcpStream,
        buffer: &mut BytesMut,
        state: &mut ConnectionState,
    ) -> crate::Result<()> {
        // Read startup packet
        stream.read_buf(buffer).await?;

        if buffer.len() < 8 {
            return Err(YamlBaseError::Protocol(
                "Invalid startup packet".to_string(),
            ));
        }

        let mut length = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
        let version = u32::from_be_bytes([buffer[4], buffer[5], buffer[6], buffer[7]]);

        // Check for SSL request
        if version == 80877103 {
            // SSL request - we don't support it
            stream.write_all(b"N").await?;
            buffer.clear();
            stream.read_buf(buffer).await?;

            // Re-read the actual startup message
            length = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
        }

        // Parse startup parameters
        let mut pos = 8;
        while pos < length - 1 {
            let key_start = pos;
            while pos < buffer.len() && buffer[pos] != 0 {
                pos += 1;
            }
            let key = std::str::from_utf8(&buffer[key_start..pos])
                .map_err(|_| YamlBaseError::Protocol("Invalid UTF-8 in startup".to_string()))?
                .to_string();
            pos += 1;

            let val_start = pos;
            while pos < buffer.len() && buffer[pos] != 0 {
                pos += 1;
            }
            let val = std::str::from_utf8(&buffer[val_start..pos])
                .map_err(|_| YamlBaseError::Protocol("Invalid UTF-8 in startup".to_string()))?
                .to_string();
            pos += 1;

            match key.as_str() {
                "user" => state.username = Some(val.clone()),
                "database" => state.database = Some(val.clone()),
                _ => {}
            }
            state.parameters.insert(key, val);
        }

        // Send authentication request
        self.send_auth_request(stream).await?;

        // Read authentication response
        buffer.clear();
        stream.read_buf(buffer).await?;

        if buffer.len() >= 5 && buffer[0] == b'p' {
            // Password message
            let msg_len = u32::from_be_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]) as usize;
            let password = self.parse_password_message(&buffer[5..5 + msg_len - 4])?;

            // Verify credentials
            debug!(
                "Auth check - Expected: {}:{}, Got: {:?}:{}",
                self.config.username, self.config.password, state.username, password
            );

            if state.username.as_deref() == Some(&self.config.username)
                && password == self.config.password
            {
                state.authenticated = true;
                self.send_auth_ok(stream, state).await?;

                // Clear the buffer after processing password message
                buffer.advance(1 + msg_len);
            } else {
                self.send_error(stream, "28P01", "Authentication failed")
                    .await?;
                return Err(YamlBaseError::Protocol("Authentication failed".to_string()));
            }
        } else {
            return Err(YamlBaseError::Protocol(
                "Expected password message".to_string(),
            ));
        }

        Ok(())
    }

    async fn send_auth_request(&self, stream: &mut TcpStream) -> crate::Result<()> {
        // Request clear text password authentication
        let mut buf = BytesMut::new();
        buf.put_u8(b'R');
        buf.put_u32(8); // Length
        buf.put_u32(3); // Clear text password

        stream.write_all(&buf).await?;
        Ok(())
    }

    async fn send_auth_ok(
        &self,
        stream: &mut TcpStream,
        _state: &ConnectionState,
    ) -> crate::Result<()> {
        // Authentication OK
        let mut buf = BytesMut::new();
        buf.put_u8(b'R');
        buf.put_u32(8);
        buf.put_u32(0);
        stream.write_all(&buf).await?;

        // Send backend key data
        buf.clear();
        buf.put_u8(b'K');
        buf.put_u32(12);
        buf.put_u32(12345); // Process ID
        buf.put_u32(67890); // Secret key
        stream.write_all(&buf).await?;

        // Send parameter status messages
        self.send_parameter_status(stream, "server_version", "14.0")
            .await?;
        self.send_parameter_status(stream, "server_encoding", "UTF8")
            .await?;
        self.send_parameter_status(stream, "client_encoding", "UTF8")
            .await?;
        self.send_parameter_status(stream, "DateStyle", "ISO, MDY")
            .await?;
        self.send_parameter_status(stream, "TimeZone", "UTC")
            .await?;

        // Ready for query
        self.send_ready_for_query(stream).await?;

        Ok(())
    }

    async fn send_parameter_status(
        &self,
        stream: &mut TcpStream,
        name: &str,
        value: &str,
    ) -> crate::Result<()> {
        let mut buf = BytesMut::new();
        buf.put_u8(b'S');
        let length = 4 + name.len() + 1 + value.len() + 1;
        buf.put_u32(length as u32);
        buf.put_slice(name.as_bytes());
        buf.put_u8(0);
        buf.put_slice(value.as_bytes());
        buf.put_u8(0);

        stream.write_all(&buf).await?;
        Ok(())
    }

    async fn send_ready_for_query(&self, stream: &mut TcpStream) -> crate::Result<()> {
        let mut buf = BytesMut::new();
        buf.put_u8(b'Z');
        buf.put_u32(5);
        buf.put_u8(b'I'); // Idle

        stream.write_all(&buf).await?;
        Ok(())
    }

    async fn handle_query(&self, stream: &mut TcpStream, query: &str) -> crate::Result<()> {
        debug!("Executing query: {}", query);

        // Parse SQL
        let statements = match parse_sql(query) {
            Ok(stmts) => stmts,
            Err(e) => {
                self.send_error(stream, "42601", &format!("Syntax error: {}", e))
                    .await?;
                self.send_ready_for_query(stream).await?;
                return Ok(());
            }
        };

        for statement in statements {
            match self.executor.execute(&statement).await {
                Ok(result) => {
                    self.send_query_result(stream, &result).await?;
                }
                Err(e) => {
                    self.send_error(stream, "XX000", &e.to_string()).await?;
                }
            }
        }

        self.send_ready_for_query(stream).await?;
        Ok(())
    }

    async fn send_query_result(
        &self,
        stream: &mut TcpStream,
        result: &crate::sql::executor::QueryResult,
    ) -> crate::Result<()> {
        // For empty results (like transaction commands), skip row description
        if !result.columns.is_empty() {
            // Send row description
            let mut buf = BytesMut::new();
            buf.put_u8(b'T');

            // Calculate length
            let mut length = 6; // 4 bytes for length + 2 bytes for field count
            for col in &result.columns {
                length += col.len() + 1 + 18; // name + null + field info
            }
            buf.put_u32(length as u32);
            buf.put_u16(result.columns.len() as u16);

            // Send field descriptions
            for (i, col) in result.columns.iter().enumerate() {
                buf.put_slice(col.as_bytes());
                buf.put_u8(0); // Null terminator
                buf.put_u32(0); // Table OID
                buf.put_u16(i as u16); // Column number

                // For simple protocol, we always send text format, so declare as text
                // to match the text data we send
                buf.put_u32(25); // text OID

                buf.put_i16(-1); // Type size
                buf.put_i32(-1); // Type modifier
                buf.put_i16(0); // Format code (text)
            }

            stream.write_all(&buf).await?;
        }

        // Send data rows
        for row in &result.rows {
            let mut buf = BytesMut::new();
            buf.put_u8(b'D');

            // Calculate row length
            let mut row_length = 6; // 4 bytes for length + 2 bytes for field count
            for val in row {
                if matches!(val, Value::Null) {
                    row_length += 4; // Just 4 bytes for NULL (-1)
                } else {
                    let val_str = val.to_string();
                    row_length += 4 + val_str.len(); // 4 bytes for value length + value
                }
            }

            buf.put_u32(row_length as u32);
            buf.put_u16(row.len() as u16);

            // Send field values
            for val in row {
                if matches!(val, Value::Null) {
                    buf.put_i32(-1); // NULL
                } else {
                    let val_str = val.to_string();
                    buf.put_i32(val_str.len() as i32);
                    buf.put_slice(val_str.as_bytes());
                }
            }

            stream.write_all(&buf).await?;
        }

        // Send command complete
        let mut buf = BytesMut::new();
        buf.put_u8(b'C');
        let tag = if result.columns.is_empty() {
            // For transaction commands, use appropriate command tag
            "BEGIN".to_string() // This is generic - ideally we'd track the actual command
        } else {
            format!("SELECT {}", result.rows.len())
        };
        buf.put_u32(4 + tag.len() as u32 + 1);
        buf.put_slice(tag.as_bytes());
        buf.put_u8(0);

        stream.write_all(&buf).await?;
        Ok(())
    }

    async fn send_error(
        &self,
        stream: &mut TcpStream,
        code: &str,
        message: &str,
    ) -> crate::Result<()> {
        let mut buf = BytesMut::new();
        buf.put_u8(b'E');

        let error_fields = vec![(b'S', "ERROR"), (b'C', code), (b'M', message)];

        let mut length = 4; // Length field
        for (_, val) in &error_fields {
            length += 1 + val.len() + 1; // Field type + value + null
        }
        length += 1; // Final null

        buf.put_u32(length as u32);

        for (field_type, val) in error_fields {
            buf.put_u8(field_type);
            buf.put_slice(val.as_bytes());
            buf.put_u8(0);
        }
        buf.put_u8(0); // End of fields

        stream.write_all(&buf).await?;
        Ok(())
    }

    fn parse_query(&self, data: &[u8]) -> crate::Result<String> {
        let end = data.iter().position(|&b| b == 0).unwrap_or(data.len());
        Ok(std::str::from_utf8(&data[..end])
            .map_err(|_| YamlBaseError::Protocol("Invalid UTF-8 in query".to_string()))?
            .to_string())
    }

    fn parse_password_message(&self, data: &[u8]) -> crate::Result<String> {
        let end = data.iter().position(|&b| b == 0).unwrap_or(data.len());
        Ok(std::str::from_utf8(&data[..end])
            .map_err(|_| YamlBaseError::Protocol("Invalid UTF-8 in password".to_string()))?
            .to_string())
    }
}
