use mysql::prelude::*;
use mysql::{Opts, OptsBuilder};
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;
use tempfile::NamedTempFile;

struct TestServer {
    process: Child,
    port: u16,
}

impl TestServer {
    fn start_mysql(yaml_content: &str) -> Self {
        // Create temporary YAML file
        let mut temp_file = NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut temp_file, yaml_content.as_bytes()).unwrap();
        let yaml_path = temp_file.path().to_str().unwrap().to_string();
        
        // Find an available port
        let port = 13306; // Use non-standard port for testing
        
        // Start the server with MySQL protocol
        let process = Command::new("cargo")
            .args(&[
                "run",
                "--",
                "-f",
                &yaml_path,
                "-p",
                &port.to_string(),
                "--protocol",
                "mysql",
                "--log-level",
                "error",
            ])
            .spawn()
            .expect("Failed to start yamlbase server");
        
        // Wait for server to start
        thread::sleep(Duration::from_secs(3));
        
        // Keep the temp file alive
        std::mem::forget(temp_file);
        
        TestServer { process, port }
    }
    
    fn connect(&self) -> mysql::Pool {
        let opts = OptsBuilder::new()
            .ip_or_hostname(Some("127.0.0.1"))
            .tcp_port(self.port)
            .user(Some("admin"))
            .pass(Some("password"))
            .db_name(Some("test_db"))
            .prefer_socket(false);
        
        for _ in 0..10 {
            if let Ok(pool) = mysql::Pool::new(Opts::from(opts.clone())) {
                return pool;
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
fn test_mysql_basic_select() {
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

    let server = TestServer::start_mysql(yaml);
    let pool = server.connect();
    let mut conn = pool.get_conn().unwrap();
    
    // Test SELECT *
    let result: Vec<(i32, String, i32)> = conn
        .query("SELECT * FROM users")
        .unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], (1, "Alice".to_string(), 30));
    assert_eq!(result[1], (2, "Bob".to_string(), 25));
    
    // Test WHERE clause
    let result: Vec<(i32, String, i32)> = conn
        .query("SELECT * FROM users WHERE age > 27")
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].1, "Alice");
    
    // Test ORDER BY
    let result: Vec<String> = conn
        .query("SELECT name FROM users ORDER BY age")
        .unwrap();
    assert_eq!(result[0], "Bob");
    assert_eq!(result[1], "Alice");
}

#[test]
fn test_mysql_data_types() {
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
        text_col: "Hello MySQL"
        bool_col: true
        decimal_col: 123.45
        timestamp_col: "2024-01-01 12:00:00"
"#;

    let server = TestServer::start_mysql(yaml);
    let pool = server.connect();
    let mut conn = pool.get_conn().unwrap();
    
    let result: Vec<(i32, String, i32, String, String)> = conn
        .query("SELECT * FROM test_types")
        .unwrap();
    assert_eq!(result.len(), 1);
    
    // Verify data types are handled correctly
    assert_eq!(result[0].0, 1);
    assert_eq!(result[0].1, "Hello MySQL");
    assert_eq!(result[0].2, 1); // Boolean as int
    assert_eq!(result[0].3, "123.45");
    assert_eq!(result[0].4, "2024-01-01 12:00:00");
}

#[test]
fn test_mysql_null_handling() {
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

    let server = TestServer::start_mysql(yaml);
    let pool = server.connect();
    let mut conn = pool.get_conn().unwrap();
    
    let result: Vec<(i32, Option<String>, String)> = conn
        .query("SELECT * FROM nullable_test")
        .unwrap();
    assert_eq!(result.len(), 2);
    
    // First row should have NULL
    assert_eq!(result[0].1, None);
    assert_eq!(result[0].2, "required");
    
    // Second row should have value
    assert_eq!(result[1].1, Some("optional".to_string()));
    assert_eq!(result[1].2, "required");
}

#[test]
fn test_mysql_limit() {
    let yaml = r#"
database:
  name: "test_db"

tables:
  items:
    columns:
      id: "INTEGER PRIMARY KEY"
      name: "VARCHAR(50)"
    data:
      - id: 1
        name: "Item1"
      - id: 2
        name: "Item2"
      - id: 3
        name: "Item3"
      - id: 4
        name: "Item4"
      - id: 5
        name: "Item5"
"#;

    let server = TestServer::start_mysql(yaml);
    let pool = server.connect();
    let mut conn = pool.get_conn().unwrap();
    
    let result: Vec<(i32, String)> = conn
        .query("SELECT * FROM items ORDER BY id LIMIT 3")
        .unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].0, 1);
    assert_eq!(result[1].0, 2);
    assert_eq!(result[2].0, 3);
}

#[test]
fn test_mysql_yaml_authentication() {
    let yaml = r#"
database:
  name: "auth_test_db"
  auth:
    username: "dbuser"
    password: "dbpass123"

tables:
  test_table:
    columns:
      id: "INTEGER PRIMARY KEY"
      name: "VARCHAR(100)"
    data:
      - id: 1
        name: "MySQL Auth Test"
"#;

    let server = TestServer::start_mysql(yaml);
    
    // Test successful authentication with YAML credentials
    let opts = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(server.port)
        .user(Some("dbuser"))
        .pass(Some("dbpass123"))
        .db_name(Some("auth_test_db"))
        .prefer_socket(false);
    
    let pool = mysql::Pool::new(Opts::from(opts)).expect("Should connect with YAML credentials");
    let mut conn = pool.get_conn().unwrap();
    
    let result: Vec<(i32, String)> = conn
        .query("SELECT * FROM test_table")
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], (1, "MySQL Auth Test".to_string()));
}

#[test]
fn test_mysql_yaml_authentication_failure() {
    let yaml = r#"
database:
  name: "auth_test_db"
  auth:
    username: "mysqluser"
    password: "mysqlpass"

tables:
  test_table:
    columns:
      id: "INTEGER PRIMARY KEY"
"#;

    let server = TestServer::start_mysql(yaml);
    
    // Test failed authentication with wrong password
    let opts = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(server.port)
        .user(Some("mysqluser"))
        .pass(Some("wrongpass"))
        .db_name(Some("auth_test_db"))
        .prefer_socket(false);
    
    let pool_result = mysql::Pool::new(Opts::from(opts));
    assert!(pool_result.is_err() || pool_result.unwrap().get_conn().is_err(), 
            "Should fail with wrong password");
    
    // Test failed authentication with wrong username
    let opts = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(server.port)
        .user(Some("wronguser"))
        .pass(Some("mysqlpass"))
        .db_name(Some("auth_test_db"))
        .prefer_socket(false);
    
    let pool_result = mysql::Pool::new(Opts::from(opts));
    assert!(pool_result.is_err() || pool_result.unwrap().get_conn().is_err(), 
            "Should fail with wrong username");
}