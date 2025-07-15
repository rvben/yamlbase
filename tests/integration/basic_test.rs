#![allow(clippy::uninlined_format_args)]

use postgres::{Client, NoTls};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;
use tempfile::NamedTempFile;

/// Get the path to the yamlbase binary, preferring pre-built binaries over cargo run
fn get_yamlbase_command() -> (String, Vec<String>) {
    // Check if YAMLBASE_TEST_BINARY env var is set (for CI)
    if let Ok(binary_path) = std::env::var("YAMLBASE_TEST_BINARY") {
        return (binary_path, vec![]);
    }
    
    // Check for pre-built binaries
    let release_binary = "target/release/yamlbase";
    let debug_binary = "target/debug/yamlbase";
    
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

struct TestServer {
    process: Child,
    port: u16,
}

impl TestServer {
    fn start(yaml_content: &str) -> Self {
        // Create temporary YAML file
        let mut temp_file = NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut temp_file, yaml_content.as_bytes()).unwrap();
        let yaml_path = temp_file.path().to_str().unwrap().to_string();
        
        // Find an available port
        let port = 15432; // Use non-standard port for testing
        
        // Start the server
        let (cmd, mut args) = get_yamlbase_command();
        args.extend(vec![
            "-f".to_string(),
            yaml_path,
            "-p".to_string(),
            port.to_string(),
            "--log-level".to_string(),
            "error".to_string(),
        ]);
        
        let process = Command::new(&cmd)
            .args(&args)
            .spawn()
            .expect("Failed to start yamlbase server");
        
        // Wait for server to start
        thread::sleep(Duration::from_secs(2));
        
        // Keep the temp file alive
        std::mem::forget(temp_file);
        
        TestServer { process, port }
    }
    
    fn connect(&self) -> Client {
        let conn_str = format!(
            "host=localhost port={} user=admin password=password dbname=test_db",
            self.port
        );
        
        for _ in 0..10 {
            if let Ok(client) = Client::connect(&conn_str, NoTls) {
                return client;
            }
            thread::sleep(Duration::from_millis(500));
        }
        
        panic!("Failed to connect to test server");
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
    }
}

#[test]
fn test_basic_select() {
    let yaml = r#"
database:
  name: "test_db"

tables:
  users:
    columns:
      id: "INTEGER PRIMARY KEY"
      name: "VARCHAR(100)"
      age: "INTEGER"
    data:
      - id: 1
        name: "Alice"
        age: 30
      - id: 2
        name: "Bob"
        age: 25
"#;

    let server = TestServer::start(yaml);
    let mut client = server.connect();
    
    // Test SELECT *
    let rows = client.query("SELECT * FROM users", &[]).unwrap();
    assert_eq!(rows.len(), 2);
    
    // Test WHERE clause
    let rows = client.query("SELECT * FROM users WHERE age > 27", &[]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(1), "Alice");
    
    // Test ORDER BY
    let rows = client.query("SELECT name FROM users ORDER BY age", &[]).unwrap();
    assert_eq!(rows[0].get::<_, String>(0), "Bob");
    assert_eq!(rows[1].get::<_, String>(0), "Alice");
}

#[test]
fn test_data_types() {
    let yaml = r#"
database:
  name: "test_db"

tables:
  test_types:
    columns:
      id: "INTEGER PRIMARY KEY"
      text_col: "TEXT"
      bool_col: "BOOLEAN"
      decimal_col: "DECIMAL(10,2)"
      timestamp_col: "TIMESTAMP"
    data:
      - id: 1
        text_col: "Hello World"
        bool_col: true
        decimal_col: 123.45
        timestamp_col: "2024-01-01 12:00:00"
"#;

    let server = TestServer::start(yaml);
    let mut client = server.connect();
    
    let rows = client.query("SELECT * FROM test_types", &[]).unwrap();
    assert_eq!(rows.len(), 1);
    
    // Verify data types are handled correctly
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, String>(1), "Hello World");
    assert_eq!(rows[0].get::<_, String>(2), "true"); // Booleans returned as strings
    assert_eq!(rows[0].get::<_, String>(3), "123.45");
}

#[test]
fn test_null_values() {
    let yaml = r#"
database:
  name: "test_db"

tables:
  nullable_test:
    columns:
      id: "INTEGER PRIMARY KEY"
      nullable_col: "VARCHAR(100)"
      not_null_col: "VARCHAR(100) NOT NULL"
    data:
      - id: 1
        nullable_col: null
        not_null_col: "required"
      - id: 2
        nullable_col: "optional"
        not_null_col: "required"
"#;

    let server = TestServer::start(yaml);
    let mut client = server.connect();
    
    let rows = client.query("SELECT * FROM nullable_test", &[]).unwrap();
    assert_eq!(rows.len(), 2);
    
    // First row should have NULL
    assert_eq!(rows[0].get::<_, Option<String>>(1), None);
    
    // Second row should have value
    assert_eq!(rows[1].get::<_, Option<String>>(1), Some("optional".to_string()));
}

#[test]
fn test_yaml_authentication() {
    let yaml = r#"
database:
  name: "auth_test_db"
  auth:
    username: "testuser"
    password: "testpass123"

tables:
  test_table:
    columns:
      id: "INTEGER PRIMARY KEY"
      name: "VARCHAR(100)"
    data:
      - id: 1
        name: "Test"
"#;

    let server = TestServer::start(yaml);
    
    // Test successful authentication with YAML credentials
    let conn_str = format!(
        "host=localhost port={} user=testuser password=testpass123 dbname=auth_test_db",
        server.port
    );
    
    let client_result = Client::connect(&conn_str, NoTls);
    assert!(client_result.is_ok(), "Should connect with YAML credentials");
    
    let mut client = client_result.unwrap();
    let rows = client.query("SELECT * FROM test_table", &[]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(1), "Test");
}

#[test]
fn test_yaml_authentication_failure() {
    let yaml = r#"
database:
  name: "auth_test_db"
  auth:
    username: "secureuser"
    password: "securepass"

tables:
  test_table:
    columns:
      id: "INTEGER PRIMARY KEY"
"#;

    let server = TestServer::start(yaml);
    
    // Test failed authentication with wrong password
    let conn_str = format!(
        "host=localhost port={} user=secureuser password=wrongpass dbname=auth_test_db",
        server.port
    );
    
    let client_result = Client::connect(&conn_str, NoTls);
    assert!(client_result.is_err(), "Should fail with wrong password");
    
    // Test failed authentication with wrong username
    let conn_str = format!(
        "host=localhost port={} user=wronguser password=securepass dbname=auth_test_db",
        server.port
    );
    
    let client_result = Client::connect(&conn_str, NoTls);
    assert!(client_result.is_err(), "Should fail with wrong username");
}

#[test]
fn test_postgresql_ssl_negotiation() {
    // This test verifies that SSL negotiation is handled correctly
    // even though we don't support SSL
    let yaml = r#"
database:
  name: "test_db"

tables:
  users:
    columns:
      id: "INTEGER PRIMARY KEY"
      name: "VARCHAR(100)"
    data:
      - id: 1
        name: "Test User"
"#;

    let server = TestServer::start(yaml);
    
    // PostgreSQL clients typically try SSL first, then fall back
    // Our server should handle this gracefully
    let mut client = server.connect();
    
    // If we get here, SSL negotiation was handled correctly
    let rows = client.query("SELECT * FROM users", &[]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(1), "Test User");
}