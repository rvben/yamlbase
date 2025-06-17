use postgres::{Client, NoTls};
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;
use tempfile::NamedTempFile;

struct TestServer {
    process: Child,
    port: u16,
}

impl TestServer {
    fn start_postgres(yaml_content: &str) -> Self {
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
                "postgres",
                "-p",
                &port.to_string(),
                "--log-level",
                "debug",
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("Failed to start yamlbase server");

        // Wait for server to start
        wait_for_postgres_port(port);

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

fn wait_for_postgres_port(port: u16) {
    println!("Waiting for PostgreSQL server to start on port {}", port);
    for i in 0..60 {
        match Client::connect(
            &format!(
                "host=localhost port={} user=yamlbase password=password dbname=test_db",
                port
            ),
            NoTls,
        ) {
            Ok(_) => {
                println!("Successfully connected after {} attempts", i + 1);
                return;
            }
            Err(e) => {
                if i % 10 == 0 {
                    println!("Connection attempt {}: {}", i + 1, e);
                }
                thread::sleep(Duration::from_millis(200));
            }
        }
    }
    panic!("PostgreSQL server failed to start on port {}", port);
}

#[test]
fn test_postgres_extended_protocol_prepared_statements() {
    let yaml = r#"
database:
  name: "test_db"
  auth:
    username: "yamlbase"
    password: "password"

tables:
  users:
    columns:
      id: "INTEGER PRIMARY KEY"
      username: "VARCHAR(50)"
      age: "INTEGER"
      active: "BOOLEAN"
    data:
      - id: 1
        username: "alice"
        age: 30
        active: true
      - id: 2
        username: "bob"
        age: 25
        active: false
      - id: 3
        username: "charlie"
        age: 35
        active: true
"#;

    let server = TestServer::start_postgres(yaml);

    let mut client = Client::connect(
        &format!(
            "host=localhost port={} user=yamlbase password=password dbname=test_db",
            server.port
        ),
        NoTls,
    )
    .expect("Failed to connect");

    // Test prepared statement with parameters
    let stmt = client
        .prepare("SELECT * FROM users WHERE age > $1 AND active = $2")
        .expect("Failed to prepare statement");

    let rows = client
        .query(&stmt, &[&27i32, &true])
        .expect("Failed to execute prepared statement");

    assert_eq!(rows.len(), 2);

    // Verify the results
    let usernames: Vec<String> = rows.iter().map(|row| row.get::<_, String>(1)).collect();

    assert!(usernames.contains(&"alice".to_string()));
    assert!(usernames.contains(&"charlie".to_string()));

    // Test reusing the same prepared statement
    let rows2 = client
        .query(&stmt, &[&30i32, &false])
        .expect("Failed to execute prepared statement second time");

    assert_eq!(rows2.len(), 0);
}

#[test]
fn test_postgres_extended_protocol_type_handling() {
    let yaml = r#"
database:
  name: "test_db"
  auth:
    username: "yamlbase"
    password: "password"

tables:
  products:
    columns:
      id: "INTEGER PRIMARY KEY"
      name: "TEXT"
      price: "DECIMAL(10,2)"
      in_stock: "BOOLEAN"
      created_date: "DATE"
      description: "TEXT"
    data:
      - id: 1
        name: "Widget"
        price: 9.99
        in_stock: true
        created_date: "2024-01-01"
        description: "A useful widget"
      - id: 2
        name: "Gadget"
        price: 19.99
        in_stock: false
        created_date: "2024-01-15"
        description: null
"#;

    let server = TestServer::start_postgres(yaml);

    let mut client = Client::connect(
        &format!(
            "host=localhost port={} user=yamlbase password=password dbname=test_db",
            server.port
        ),
        NoTls,
    )
    .expect("Failed to connect");

    // Test various parameter types
    let stmt = client
        .prepare("SELECT * FROM products WHERE price < $1 AND in_stock = $2")
        .expect("Failed to prepare statement");

    let rows = client
        .query(&stmt, &[&15.00f64, &true])
        .expect("Failed to execute query");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(1), "Widget");

    // Test NULL handling
    let null_stmt = client
        .prepare("SELECT * FROM products WHERE description IS NULL")
        .expect("Failed to prepare NULL check statement");

    let null_rows = client
        .query(&null_stmt, &[])
        .expect("Failed to execute NULL query");

    assert_eq!(null_rows.len(), 1);
    assert_eq!(null_rows[0].get::<_, i32>(0), 2);
}

#[test]
fn test_postgres_extended_protocol_multiple_statements() {
    let yaml = r#"
database:
  name: "test_db"
  auth:
    username: "yamlbase"
    password: "password"

tables:
  orders:
    columns:
      id: "INTEGER PRIMARY KEY"
      customer_name: "VARCHAR(100)"
      total: "DECIMAL(10,2)"
      status: "VARCHAR(20)"
    data:
      - id: 1
        customer_name: "Alice Smith"
        total: 99.99
        status: "pending"
      - id: 2
        customer_name: "Bob Jones"
        total: 149.99
        status: "shipped"
      - id: 3
        customer_name: "Charlie Brown"
        total: 79.99
        status: "pending"
"#;

    let server = TestServer::start_postgres(yaml);

    let mut client = Client::connect(
        &format!(
            "host=localhost port={} user=yamlbase password=password dbname=test_db",
            server.port
        ),
        NoTls,
    )
    .expect("Failed to connect");

    // Prepare multiple statements
    let stmt1 = client
        .prepare("SELECT COUNT(*) FROM orders WHERE status = $1")
        .expect("Failed to prepare count statement");

    let stmt2 = client
        .prepare("SELECT SUM(total) FROM orders WHERE status = $1")
        .expect("Failed to prepare sum statement");

    let stmt3 = client
        .prepare("SELECT * FROM orders WHERE customer_name LIKE $1 ORDER BY id")
        .expect("Failed to prepare LIKE statement");

    // Execute multiple prepared statements
    let pending_count: i64 = client
        .query_one(&stmt1, &[&"pending"])
        .expect("Failed to count pending orders")
        .get(0);

    assert_eq!(pending_count, 2);

    let pending_total: String = client
        .query_one(&stmt2, &[&"pending"])
        .expect("Failed to sum pending orders")
        .get(0);

    assert_eq!(pending_total, "179.98");

    // Test LIKE with prepared statement
    let rows = client
        .query(&stmt3, &[&"%Brown%"])
        .expect("Failed to execute LIKE query");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(1), "Charlie Brown");
}

#[test]
fn test_postgres_extended_protocol_performance() {
    let yaml = r#"
database:
  name: "test_db"
  auth:
    username: "yamlbase"
    password: "password"

tables:
  users:
    columns:
      id: "INTEGER PRIMARY KEY"
      username: "VARCHAR(50)"
      score: "INTEGER"
    data:
"#;

    // Generate 1000 users
    let mut yaml_with_data = yaml.to_string();
    for i in 1..=1000 {
        yaml_with_data.push_str(&format!(
            "        - id: {}\n          username: \"user{}\"\n          score: {}\n",
            i,
            i,
            i % 100
        ));
    }

    let server = TestServer::start_postgres(yaml_with_data.as_str());

    let mut client = Client::connect(
        &format!(
            "host=localhost port={} user=yamlbase password=password dbname=test_db",
            server.port
        ),
        NoTls,
    )
    .expect("Failed to connect");

    // Prepare statement once
    let stmt = client
        .prepare("SELECT * FROM users WHERE id = $1")
        .expect("Failed to prepare statement");

    // Execute many times with different parameters
    let start = std::time::Instant::now();
    for i in [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000] {
        let rows = client.query(&stmt, &[&i]).expect("Failed to execute query");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, i32>(0), i);
    }
    let duration = start.elapsed();

    println!("10 prepared statement executions took: {:?}", duration);

    // The prepared statements should be fast due to index usage
    assert!(
        duration.as_millis() < 100,
        "Prepared statements took too long: {:?}",
        duration
    );
}
