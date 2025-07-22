use std::path::Path;
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
        let (cmd, mut args) = get_yamlbase_command();
        args.extend(vec![
            "-f".to_string(),
            yaml_path,
            "--protocol".to_string(),
            "postgres".to_string(),
            "-p".to_string(),
            port.to_string(),
            "--log-level".to_string(),
            "debug".to_string(),
        ]);

        let process = Command::new(&cmd)
            .args(&args)
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
    println!("Waiting for PostgreSQL server to start on port {port}");
    for i in 1..=30 {
        match std::net::TcpStream::connect(format!("127.0.0.1:{port}")) {
            Ok(_) => {
                println!("Successfully connected after {i} attempts");
                thread::sleep(Duration::from_millis(100)); // Give the server a bit more time
                return;
            }
            Err(e) => {
                println!("Connection attempt {i}: {e}");
                thread::sleep(Duration::from_millis(200));
            }
        }
    }
    panic!("Failed to connect to PostgreSQL server on port {port}");
}

fn execute_query(port: u16, query: &str) -> String {
    let output = Command::new("psql")
        .args([
            "-h",
            "localhost",
            "-p",
            &port.to_string(),
            "-U",
            "yamlbase",
            "-d",
            "test_db",
            "-t", // Tuples only (no headers)
            "-A", // Unaligned output (no column padding)
            "-c",
            query,
        ])
        .env("PGPASSWORD", "password")
        .output()
        .expect("Failed to execute psql");

    if !output.status.success() {
        panic!(
            "psql query failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    String::from_utf8(output.stdout)
        .expect("Invalid UTF-8 in psql output")
        .trim()
        .to_string()
}

#[test]
fn test_postgres_string_functions() {
    let yaml = r#"
database:
  name: "test_db"
  auth:
    username: "yamlbase"
    password: "password"

tables:
  test_data:
    columns:
      id: "INTEGER PRIMARY KEY"
      name: "TEXT"
      description: "TEXT"
    data:
      - id: 1
        name: "Alice Johnson"
        description: "Senior Developer"
      - id: 2
        name: "Bob Smith"
        description: "Project Manager"
      - id: 3
        name: "Charlie Brown"
        description: "UX Designer"
"#;

    let server = TestServer::start_postgres(yaml);

    // Test string functions
    let result = execute_query(server.port, "SELECT LEFT('Hello World', 5)");
    assert_eq!(result, "Hello");

    let result = execute_query(server.port, "SELECT RIGHT('Hello World', 5)");
    assert_eq!(result, "World");

    let result = execute_query(server.port, "SELECT POSITION('World', 'Hello World')");
    assert_eq!(result, "7");

    let result = execute_query(server.port, "SELECT POSITION('🎊', '🎉🎊🎈')");
    assert_eq!(result, "2"); // Character position

    // Test LTRIM - psql trims trailing whitespace, so test with concatenation
    let result = execute_query(server.port, "SELECT CONCAT(LTRIM('  hello  '), 'X')");
    assert_eq!(result, "hello  X");

    // Test RTRIM
    let result = execute_query(server.port, "SELECT CONCAT('X', RTRIM('  hello  '))");
    assert_eq!(result, "X  hello");

    let result = execute_query(server.port, "SELECT CONCAT('Hello', ' ', 'World')");
    assert_eq!(result, "Hello World");
}

#[test]
fn test_postgres_math_functions() {
    let yaml = r#"
database:
  name: "test_db"
  auth:
    username: "yamlbase"
    password: "password"

tables:
  numbers:
    columns:
      id: "INTEGER PRIMARY KEY"
      value: "DOUBLE"
    data:
      - id: 1
        value: 3.14159
      - id: 2
        value: -2.5
      - id: 3
        value: 10.7
"#;

    let server = TestServer::start_postgres(yaml);

    // Test math functions
    let result = execute_query(server.port, "SELECT ROUND(3.14159)");
    assert_eq!(result, "3");

    let result = execute_query(server.port, "SELECT ROUND(3.14159, 2)");
    assert_eq!(result, "3.14");

    let result = execute_query(server.port, "SELECT CEIL(3.1)");
    assert_eq!(result, "4");

    let result = execute_query(server.port, "SELECT FLOOR(3.9)");
    assert_eq!(result, "3");

    let result = execute_query(server.port, "SELECT ABS(-5)");
    assert_eq!(result, "5");

    let result = execute_query(server.port, "SELECT ABS(-3.14)");
    assert_eq!(result, "3.14");

    let result = execute_query(server.port, "SELECT MOD(10, 3)");
    assert_eq!(result, "1");

    let result = execute_query(
        server.port,
        "SELECT ROUND(value, 1) FROM numbers WHERE id = 1",
    );
    assert_eq!(result, "3.1");
}

#[test]
fn test_postgres_cast_function() {
    let yaml = r#"
database:
  name: "test_db"
  auth:
    username: "yamlbase"
    password: "password"

tables:
  test_data:
    columns:
      id: "INTEGER PRIMARY KEY"
    data:
      - id: 1
"#;

    let server = TestServer::start_postgres(yaml);

    // Test CAST function
    let result = execute_query(server.port, "SELECT CAST('123' AS INTEGER)");
    assert_eq!(result, "123");

    let result = execute_query(server.port, "SELECT CAST(45.67 AS INTEGER)");
    assert_eq!(result, "45");

    let result = execute_query(server.port, "SELECT CAST('3.14' AS FLOAT)");
    assert_eq!(result, "3.14");

    let result = execute_query(server.port, "SELECT CAST(42 AS TEXT)");
    assert_eq!(result, "42");

    let result = execute_query(server.port, "SELECT CAST('2025-01-15' AS DATE)");
    assert_eq!(result, "2025-01-15");

    let result = execute_query(server.port, "SELECT CAST(1 AS BOOLEAN)");
    assert_eq!(result, "true");

    let result = execute_query(server.port, "SELECT CAST('true' AS BOOLEAN)");
    assert_eq!(result, "true");
}

#[test]
fn test_postgres_right_join() {
    let yaml = r#"
database:
  name: "test_db"
  auth:
    username: "yamlbase"
    password: "password"

tables:
  employees:
    columns:
      id: "INTEGER PRIMARY KEY"
      name: "TEXT"
      dept_id: "INTEGER"
    data:
      - id: 1
        name: "Alice"
        dept_id: 1
      - id: 2
        name: "Bob"
        dept_id: 2
      - id: 3
        name: "Charlie"
        dept_id: null

  departments:
    columns:
      id: "INTEGER PRIMARY KEY"
      name: "TEXT"
    data:
      - id: 1
        name: "Engineering"
      - id: 2
        name: "Sales"
      - id: 3
        name: "Marketing"
"#;

    let server = TestServer::start_postgres(yaml);

    // Test RIGHT JOIN - just verify we can execute it without error
    let result = execute_query(
        server.port,
        "SELECT d.name FROM employees e RIGHT JOIN departments d ON e.dept_id = d.id WHERE e.name IS NULL",
    );
    assert_eq!(result, "Marketing"); // Only Marketing has no employees
}

#[test]
fn test_postgres_date_functions() {
    let yaml = r#"
database:
  name: "test_db"
  auth:
    username: "yamlbase"
    password: "password"

tables:
  events:
    columns:
      id: "INTEGER PRIMARY KEY"
      event_date: "DATE"
    data:
      - id: 1
        event_date: "2025-01-15"
      - id: 2
        event_date: "2025-12-25"
"#;

    let server = TestServer::start_postgres(yaml);

    // Test date functions
    let result = execute_query(server.port, "SELECT CURRENT_DATE");
    let today = chrono::Local::now().naive_local().date();
    assert_eq!(result, today.to_string());

    let result = execute_query(server.port, "SELECT CURRENT_TIMESTAMP");
    // Parse timestamp and check it's recent
    let current_timestamp = chrono::NaiveDateTime::parse_from_str(&result, "%Y-%m-%d %H:%M:%S")
        .expect("Failed to parse timestamp");
    let now = chrono::Local::now().naive_local();
    // Check that timestamp is within 1 minute of now
    assert!((current_timestamp.and_utc().timestamp() - now.and_utc().timestamp()).abs() < 60);

    // Test DATE_FORMAT with column values
    let result = execute_query(server.port, "SELECT event_date FROM events WHERE id = 1");
    assert_eq!(result, "2025-01-15");

    // Test DATE_FORMAT with literal date
    let result = execute_query(server.port, "SELECT DATE_FORMAT('2025-01-15', '%d/%m/%Y')");
    assert_eq!(result, "15/01/2025");
}
