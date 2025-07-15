use mysql::prelude::*;
use mysql::{Conn, OptsBuilder};
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
        let port = get_free_port();

        // Start the server
        let process = Command::new("cargo")
            .args(&[
                "run",
                "--",
                "-f",
                &yaml_path,
                "--protocol",
                "mysql",
                "-p",
                &port.to_string(),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("Failed to start yamlbase server");

        // Wait for server to start
        wait_for_mysql_port(port);

        // Keep the temp file alive
        std::mem::forget(temp_file);

        TestServer { process, port }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
    }
}

fn get_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

fn wait_for_mysql_port(port: u16) {
    for _ in 0..30 {
        if std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("MySQL server failed to start on port {}", port);
}

#[test]
fn test_mysql_8_caching_sha2_password() {
    let yaml = r#"
database:
  name: test_db

tables:
  users:
    columns:
      id: "INTEGER PRIMARY KEY"
      username: "VARCHAR(50)"
      email: "TEXT"
    data:
      - id: 1
        username: "alice"
        email: "alice@example.com"
      - id: 2
        username: "bob"
        email: "bob@example.com"
"#;

    let server = TestServer::start_mysql(yaml);

    // Connect with MySQL 8.0 client (defaults to caching_sha2_password)
    let opts = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(server.port)
        .user(Some("admin"))
        .pass(Some("password"))
        .db_name(Some("test_db"));

    let mut conn = Conn::new(opts).expect("Failed to connect to MySQL");

    // Test basic query
    let result: Vec<(i32, String, String)> = conn
        .query("SELECT id, username, email FROM users ORDER BY id")
        .expect("Failed to execute query");

    assert_eq!(result.len(), 2);
    assert_eq!(
        result[0],
        (1, "alice".to_string(), "alice@example.com".to_string())
    );
    assert_eq!(
        result[1],
        (2, "bob".to_string(), "bob@example.com".to_string())
    );

    // Test WHERE clause with direct query (prepared statements not yet implemented)
    let result: Vec<(i32, String, String)> = conn
        .query("SELECT id, username, email FROM users WHERE id = 1")
        .expect("Failed to execute WHERE query");

    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0],
        (1, "alice".to_string(), "alice@example.com".to_string())
    );
}

#[test]
fn test_mysql_8_with_auth_in_yaml() {
    let yaml = r#"
database:
  name: myapp
  auth:
    username: "testuser"
    password: "testpass123"

tables:
  products:
    columns:
      id: "INTEGER PRIMARY KEY"
      name: "VARCHAR(100)"
      price: "DECIMAL(10,2)"
    data:
      - id: 1
        name: "Widget"
        price: 9.99
      - id: 2
        name: "Gadget"
        price: 19.99
"#;

    let server = TestServer::start_mysql(yaml);

    // Connect with credentials from YAML
    let opts = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(server.port)
        .user(Some("testuser"))
        .pass(Some("testpass123"))
        .db_name(Some("myapp"));

    let mut conn = Conn::new(opts).expect("Failed to connect to MySQL");

    // Test query with decimal values
    let result: Vec<(i32, String, String)> = conn
        .query("SELECT id, name, price FROM products WHERE price < 15")
        .expect("Failed to execute query");

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].1, "Widget");
    assert_eq!(result[0].2, "9.99");
}

#[test]
fn test_mysql_8_system_variables() {
    let yaml = r#"
database:
  name: test_db

tables:
  dummy:
    columns:
      id: "INTEGER"
    data:
      - id: 1
"#;

    let server = TestServer::start_mysql(yaml);

    let opts = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(server.port)
        .user(Some("admin"))
        .pass(Some("password"))
        .db_name(Some("test_db"));

    let mut conn = Conn::new(opts).expect("Failed to connect to MySQL");

    // Test system variables that MySQL 8.0 clients commonly query
    let version: String = conn
        .query_first("SELECT @@version")
        .expect("Failed to query version")
        .expect("No version returned");

    assert!(version.contains("yamlbase"));

    // Test multiple system variables
    let result: Vec<(String, String)> = conn
        .query("SELECT @@version, @@version_comment")
        .expect("Failed to query system variables");

    assert_eq!(result.len(), 1);
    assert!(result[0].0.contains("yamlbase"));
}

#[test]
fn test_mysql_8_null_handling() {
    let yaml = r#"
database:
  name: test_db

tables:
  users:
    columns:
      id: "INTEGER PRIMARY KEY"
      name: "VARCHAR(50)"
      email: "VARCHAR(100)"
    data:
      - id: 1
        name: "alice"
        email: "alice@example.com"
      - id: 2
        name: "bob"
        email: null
"#;

    let server = TestServer::start_mysql(yaml);

    let opts = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(server.port)
        .user(Some("admin"))
        .pass(Some("password"))
        .db_name(Some("test_db"));

    let mut conn = Conn::new(opts).expect("Failed to connect to MySQL");

    // Test NULL values
    let result: Vec<(i32, String, Option<String>)> = conn
        .query("SELECT id, name, email FROM users ORDER BY id")
        .expect("Failed to execute query");

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].2, Some("alice@example.com".to_string()));
    assert_eq!(result[1].2, None);

    // Test IS NULL condition
    let null_count: i32 = conn
        .query_first("SELECT COUNT(*) FROM users WHERE email IS NULL")
        .expect("Failed to count NULL emails")
        .expect("No count returned");

    assert_eq!(null_count, 1);
}
