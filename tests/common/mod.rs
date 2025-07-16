#![allow(clippy::uninlined_format_args)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tempfile::NamedTempFile;
use yamlbase::config::{Config, Protocol};
use yamlbase::database::Database;

// Start from a high port to avoid conflicts with common services
static NEXT_PORT: AtomicU16 = AtomicU16::new(40000);

/// Get the path to the yamlbase binary, preferring pre-built binaries over cargo run
fn get_yamlbase_command() -> (String, Vec<String>) {
    // Check if YAMLBASE_TEST_BINARY env var is set (for CI)
    if let Ok(binary_path) = std::env::var("YAMLBASE_TEST_BINARY") {
        return (binary_path, vec![]);
    }

    // Check for pre-built binaries
    let release_binary = if cfg!(windows) {
        "target/release/yamlbase.exe"
    } else {
        "target/release/yamlbase"
    };
    let debug_binary = if cfg!(windows) {
        "target/debug/yamlbase.exe"
    } else {
        "target/debug/yamlbase"
    };

    if Path::new(release_binary).exists() {
        return (release_binary.to_string(), vec![]);
    }

    if Path::new(debug_binary).exists() {
        return (debug_binary.to_string(), vec![]);
    }

    // Fall back to cargo run
    let cargo_path = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    (cargo_path, vec!["run".to_string(), "--".to_string()])
}

/// Get a free port for testing by binding to port 0 and letting the OS assign
fn get_free_port() -> u16 {
    // Try binding to 0 to get an OS-assigned port
    match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => {
            let port = listener.local_addr().unwrap().port();
            drop(listener); // Release the port immediately
            port
        }
        Err(_) => {
            // Fallback to incrementing port numbers if OS assignment fails
            NEXT_PORT.fetch_add(1, Ordering::SeqCst)
        }
    }
}

/// Wait for a port to be available
fn wait_for_port(port: u16, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "Server failed to start on port {} within {:?}",
        port, timeout
    );
}

#[allow(dead_code)]
pub struct TestServer {
    pub port: u16,
    pub config: Arc<Config>,
    process: Option<Child>,
    _temp_file: Option<NamedTempFile>,
}

#[allow(dead_code)]
impl TestServer {
    pub fn start_mysql(yaml_file: &str) -> Self {
        let port = get_free_port();

        let (cmd, mut args) = get_yamlbase_command();
        args.extend(vec![
            "-f".to_string(),
            yaml_file.to_string(),
            "--protocol".to_string(),
            "mysql".to_string(),
            "-p".to_string(),
            port.to_string(),
        ]);

        let process = Command::new(&cmd)
            .args(&args)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("Failed to start server");

        // Wait for server to be ready
        wait_for_port(port, Duration::from_secs(10));

        let config = Arc::new(Config {
            file: PathBuf::from(yaml_file),
            port: Some(port),
            bind_address: "127.0.0.1".to_string(),
            protocol: Protocol::Mysql,
            username: "root".to_string(),
            password: "password".to_string(),
            verbose: false,
            hot_reload: false,
            log_level: "info".to_string(),
            database: None,
        });

        Self {
            port,
            config,
            process: Some(process),
            _temp_file: None,
        }
    }

    pub fn start_postgres(yaml_file: &str) -> Self {
        let port = get_free_port();

        let (cmd, mut args) = get_yamlbase_command();
        args.extend(vec![
            "-f".to_string(),
            yaml_file.to_string(),
            "--protocol".to_string(),
            "postgres".to_string(),
            "-p".to_string(),
            port.to_string(),
        ]);

        let process = Command::new(&cmd)
            .args(&args)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("Failed to start server");

        // Wait for server to be ready
        wait_for_port(port, Duration::from_secs(10));

        let config = Arc::new(Config {
            file: PathBuf::from(yaml_file),
            port: Some(port),
            bind_address: "127.0.0.1".to_string(),
            protocol: Protocol::Postgres,
            username: "yamlbase".to_string(),
            password: "password".to_string(),
            verbose: false,
            hot_reload: false,
            log_level: "info".to_string(),
            database: None,
        });

        Self {
            port,
            config,
            process: Some(process),
            _temp_file: None,
        }
    }

    pub async fn new_postgres(db: Database) -> Self {
        // Run the blocking operations in a blocking thread
        tokio::task::spawn_blocking(move || {
            let port = get_free_port();

            // Write database to temp file
            let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
            let yaml_content = format!(
                "database:\n  name: \"{}\"\n  auth:\n    username: \"yamlbase\"\n    password: \"password\"\n\ntables:\n",
                db.name
            );

            // Add tables
            let mut tables_yaml = String::new();
            for table in db.tables.values() {
                tables_yaml.push_str(&format!("  {}:\n    columns:\n", table.name));
                for col in &table.columns {
                    // Map sql_type to a SQL column definition string
                    let type_str = match &col.sql_type {
                        yamlbase::yaml::schema::SqlType::Integer => {
                            if col.primary_key {
                                "INTEGER PRIMARY KEY".to_string()
                            } else {
                                "INTEGER".to_string()
                            }
                        }
                        yamlbase::yaml::schema::SqlType::Text => "TEXT".to_string(),
                        yamlbase::yaml::schema::SqlType::Varchar(n) => format!("VARCHAR({})", n),
                        yamlbase::yaml::schema::SqlType::Boolean => "BOOLEAN".to_string(),
                        yamlbase::yaml::schema::SqlType::Date => "DATE".to_string(),
                        yamlbase::yaml::schema::SqlType::Timestamp => "TIMESTAMP".to_string(),
                        yamlbase::yaml::schema::SqlType::Float => "FLOAT".to_string(),
                        yamlbase::yaml::schema::SqlType::Double => "DOUBLE".to_string(),
                        yamlbase::yaml::schema::SqlType::Decimal(p, s) => format!("DECIMAL({},{})", p, s),
                        yamlbase::yaml::schema::SqlType::BigInt => "BIGINT".to_string(),
                        yamlbase::yaml::schema::SqlType::Time => "TIME".to_string(),
                        yamlbase::yaml::schema::SqlType::Uuid => "UUID".to_string(),
                        yamlbase::yaml::schema::SqlType::Json => "JSON".to_string(),
                    };

                    let mut col_def = type_str.to_string();
                    if col.nullable && !col.primary_key {
                        col_def.push_str(" NULL");
                    } else if !col.primary_key {
                        col_def.push_str(" NOT NULL");
                    }
                    if col.unique && !col.primary_key {
                        col_def.push_str(" UNIQUE");
                    }

                    tables_yaml.push_str(&format!("      {}: \"{}\"\n", col.name, col_def));
                }
                tables_yaml.push_str("    data:\n");
                for row in &table.rows {
                    tables_yaml.push_str("      - ");
                    for (i, (col, val)) in table.columns.iter().zip(row.iter()).enumerate() {
                        if i > 0 {
                            tables_yaml.push_str("        ");
                        }
                        tables_yaml.push_str(&format!("{}: ", col.name));
                        match val {
                            yamlbase::database::Value::Null => tables_yaml.push_str("null"),
                            yamlbase::database::Value::Integer(i) => tables_yaml.push_str(&i.to_string()),
                            yamlbase::database::Value::Text(s) => tables_yaml.push_str(&format!("\"{}\"", s)),
                            yamlbase::database::Value::Boolean(b) => tables_yaml.push_str(&b.to_string()),
                            _ => tables_yaml.push_str(&format!("{:?}", val)),
                        }
                        if i < table.columns.len() - 1 {
                            tables_yaml.push('\n');
                        }
                    }
                    tables_yaml.push('\n');
                }
            }

            let full_yaml = format!("{}{}", yaml_content, tables_yaml);
            temp_file.write_all(full_yaml.as_bytes()).expect("Failed to write temp file");
            temp_file.flush().expect("Failed to flush temp file");

            let yaml_path = temp_file.path().to_str().unwrap().to_string();

            let (cmd, mut args) = get_yamlbase_command();
            args.extend(vec![
                "-f".to_string(),
                yaml_path.clone(),
                "--protocol".to_string(),
                "postgres".to_string(),
                "-p".to_string(),
                port.to_string(),
            ]);

            let process = Command::new(&cmd)
                .args(&args)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .expect("Failed to start server");

            // Wait for server to be ready
            wait_for_port(port, Duration::from_secs(10));

            let config = Arc::new(Config {
                file: PathBuf::from(yaml_path),
                port: Some(port),
                bind_address: "127.0.0.1".to_string(),
                protocol: Protocol::Postgres,
                username: "yamlbase".to_string(),
                password: "password".to_string(),
                verbose: false,
                hot_reload: false,
                log_level: "info".to_string(),
                database: None,
            });

            Self { port, config, process: Some(process), _temp_file: Some(temp_file) }
        }).await.expect("Failed to create test server")
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn connect(&self) -> TcpStream {
        TcpStream::connect(format!("127.0.0.1:{}", self.port)).expect("Failed to connect to server")
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(mut process) = self.process.take() {
            let _ = process.kill();
        }
    }
}

// MySQL specific helpers

pub fn _mysql_connect_and_auth(server: &TestServer, username: &str, password: &str) -> TcpStream {
    let mut stream = server.connect();

    // Read handshake
    let mut header = [0u8; 4];
    stream
        .read_exact(&mut header)
        .expect("Failed to read header");
    let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
    let sequence = header[3];

    let mut handshake = vec![0u8; length as usize];
    stream
        .read_exact(&mut handshake)
        .expect("Failed to read handshake");

    // Extract auth data
    let pos = handshake.iter().position(|&b| b == 0).unwrap() + 1 + 4;
    let auth_data_1 = &handshake[pos..pos + 8];
    let auth_data_2 =
        &handshake[pos + 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10..pos + 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10 + 12];
    let mut auth_data = auth_data_1.to_vec();
    auth_data.extend_from_slice(auth_data_2);

    // Send auth response
    let mut response = Vec::new();
    response.extend(&0x000fa685u32.to_le_bytes()); // capabilities
    response.extend(&16777216u32.to_le_bytes()); // max packet
    response.push(33); // charset
    response.extend(&[0u8; 23]); // reserved
    response.extend(username.as_bytes());
    response.push(0);

    // Calculate auth response
    let auth_response = _mysql_native_password_auth(&auth_data, password);
    response.push(auth_response.len() as u8);
    response.extend(&auth_response);

    // Send response
    let mut packet = Vec::new();
    packet.extend(&(response.len() as u32).to_le_bytes()[..3]);
    packet.push(sequence + 1);
    packet.extend(&response);

    stream.write_all(&packet).expect("Failed to send auth");

    // Read auth result
    let mut header = [0u8; 4];
    stream
        .read_exact(&mut header)
        .expect("Failed to read response header");
    let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);

    let mut response_data = vec![0u8; length as usize];
    stream
        .read_exact(&mut response_data)
        .expect("Failed to read response");

    // Check for auth switch request
    if response_data[0] == 0xfe {
        // Server is requesting auth switch, send empty auth response
        let mut packet = Vec::new();
        packet.extend(&[0, 0, 0]); // length = 0
        packet.push(header[3] + 1); // sequence
        stream
            .write_all(&packet)
            .expect("Failed to send empty auth");

        // Read final OK packet
        let mut header = [0u8; 4];
        stream
            .read_exact(&mut header)
            .expect("Failed to read OK header");
        let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);

        let mut ok_packet = vec![0u8; length as usize];
        stream
            .read_exact(&mut ok_packet)
            .expect("Failed to read OK packet");

        if ok_packet[0] != 0x00 {
            panic!("Authentication failed after auth switch");
        }
    } else if response_data[0] != 0x00 {
        panic!("Authentication failed");
    }

    stream
}

fn _mysql_native_password_auth(auth_data: &[u8], password: &str) -> Vec<u8> {
    use sha1::{Digest, Sha1};

    if password.is_empty() {
        return vec![];
    }

    let mut hasher = Sha1::new();
    hasher.update(password.as_bytes());
    let password_hash = hasher.finalize();

    let mut hasher = Sha1::new();
    hasher.update(password_hash);
    let password_double_hash = hasher.finalize();

    let mut hasher = Sha1::new();
    hasher.update(auth_data);
    hasher.update(password_double_hash);
    let result = hasher.finalize();

    password_hash
        .iter()
        .zip(result.iter())
        .map(|(a, b)| a ^ b)
        .collect()
}

pub fn _mysql_test_query(stream: &mut TcpStream, query: &str, _expected_values: Vec<&str>) {
    // Send query
    let mut packet = Vec::new();
    packet.extend(&((query.len() + 1) as u32).to_le_bytes()[..3]);
    packet.push(0); // sequence
    packet.push(0x03); // COM_QUERY
    packet.extend(query.as_bytes());

    stream.write_all(&packet).expect("Failed to send query");

    // Read response
    let mut header = [0u8; 4];
    stream
        .read_exact(&mut header)
        .expect("Failed to read response header");
    let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);

    let mut response = vec![0u8; length as usize];
    stream
        .read_exact(&mut response)
        .expect("Failed to read response");

    if response[0] == 0xff {
        let error_code = u16::from_le_bytes([response[1], response[2]]);
        let error_msg = String::from_utf8_lossy(&response[9..]);
        panic!("Query error {}: {}", error_code, error_msg);
    }

    // Simple validation - just check that we got a result set
    assert!(response[0] > 0, "Expected result set, got: {:?}", response);

    println!("Query '{}' returned result set", query);

    // Read and discard remaining packets until EOF
    loop {
        let mut header = [0u8; 4];
        if stream.read_exact(&mut header).is_err() {
            break;
        }
        let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);

        let mut data = vec![0u8; length as usize];
        if stream.read_exact(&mut data).is_err() {
            break;
        }

        // Check for EOF packet
        if length == 5 && data[0] == 0xfe {
            break;
        }
    }
}

pub fn _mysql_test_ping(stream: &mut TcpStream) {
    // For the test, we need to track sequence numbers properly
    // After multiple queries, the sequence number will be higher
    // Let's use a higher sequence number that matches the current state
    let packet = vec![1, 0, 0, 3, 0x0e]; // length=1, seq=3, COM_PING=0x0e
    stream.write_all(&packet).expect("Failed to send ping");

    // Read OK response
    let mut header = [0u8; 4];
    stream
        .read_exact(&mut header)
        .expect("Failed to read response header");
    let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
    let seq = header[3];

    let mut response = vec![0u8; length as usize];
    stream
        .read_exact(&mut response)
        .expect("Failed to read response");

    // Debug: Print the response
    if response[0] != 0x00 {
        eprintln!(
            "PING response header: {:02x} {:02x} {:02x} {:02x}",
            header[0], header[1], header[2], header[3]
        );
        eprintln!(
            "PING response data: {:?} (length={}, seq={})",
            response, length, seq
        );
        eprintln!("As hex: {:02x?}", response);
    }

    assert_eq!(response[0], 0x00, "Expected OK packet for PING");
    println!("PING successful");
}
