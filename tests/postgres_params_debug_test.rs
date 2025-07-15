use bytes::{BufMut, BytesMut};
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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

#[tokio::test]
async fn test_postgres_parameter_handling() {
    // Start a simple yamlbase server
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
      age: "INTEGER"
    data:
      - id: 1
        age: 30
      - id: 2
        age: 25
"#;

    // Create temporary YAML file
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(temp_file.path(), yaml).unwrap();

    // Find a free port
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    // Start the server
    let temp_path = temp_file.path().to_str().unwrap().to_string();
    let server = tokio::spawn(async move {
        let (cmd, mut args) = get_yamlbase_command();
        args.extend(vec![
            "-f".to_string(),
            temp_path,
            "--protocol".to_string(),
            "postgres".to_string(),
            "-p".to_string(),
            port.to_string(),
            "--log-level".to_string(),
            "debug".to_string(),
        ]);
        
        let _ = tokio::process::Command::new(&cmd)
            .args(&args)
            .kill_on_drop(true)
            .spawn()
            .unwrap()
            .wait()
            .await;
    });

    // Wait for server to start
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Connect and authenticate
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Send startup message
    let mut buf = BytesMut::new();
    buf.put_u32(0); // Length placeholder
    buf.put_u32(196608); // Protocol version 3.0
    buf.put_slice(b"user\0yamlbase\0");
    buf.put_slice(b"database\0test_db\0");
    buf.put_u8(0); // End of parameters

    // Update length
    let len = buf.len() as u32;
    buf[0..4].copy_from_slice(&len.to_be_bytes());

    stream.write_all(&buf).await.unwrap();

    // Read and handle auth
    let mut response = vec![0u8; 4096];
    let n = stream.read(&mut response).await.unwrap();
    println!("Auth request: {} bytes", n);

    // Send password
    buf.clear();
    buf.put_u8(b'p');
    buf.put_u32(13);
    buf.put_slice(b"password\0");
    stream.write_all(&buf).await.unwrap();

    // Read auth response
    let n = stream.read(&mut response).await.unwrap();
    println!("Auth response: {} bytes", n);

    // Parse message
    buf.clear();
    buf.put_u8(b'P');
    let stmt_name = "";
    let query = "SELECT * FROM users WHERE age > $1";
    let param_types = vec![23u32]; // INTEGER OID

    let msg_len = 4 + stmt_name.len() + 1 + query.len() + 1 + 2 + (param_types.len() * 4);
    buf.put_u32(msg_len as u32);
    buf.put_slice(stmt_name.as_bytes());
    buf.put_u8(0);
    buf.put_slice(query.as_bytes());
    buf.put_u8(0);
    buf.put_u16(param_types.len() as u16);
    for oid in param_types {
        buf.put_u32(oid);
    }

    stream.write_all(&buf).await.unwrap();

    // Bind message
    buf.clear();
    buf.put_u8(b'B');
    let portal_name = "";
    let param_value = 27i32;

    let msg_len = 4 + portal_name.len() + 1 + stmt_name.len() + 1
        + 2 + 2 // format code counts
        + 2 + 4 + 4 // parameter count + length + value
        + 2; // result format codes

    buf.put_u32(msg_len as u32);
    buf.put_slice(portal_name.as_bytes());
    buf.put_u8(0);
    buf.put_slice(stmt_name.as_bytes());
    buf.put_u8(0);
    buf.put_u16(0); // 0 parameter format codes (use default)
    buf.put_u16(1); // 1 parameter value
    buf.put_u32(4); // parameter length
    buf.put_i32(param_value); // parameter value
    buf.put_u16(0); // 0 result format codes (use default)

    stream.write_all(&buf).await.unwrap();

    // Execute
    buf.clear();
    buf.put_u8(b'E');
    buf.put_u32(4 + portal_name.len() as u32 + 1 + 4);
    buf.put_slice(portal_name.as_bytes());
    buf.put_u8(0);
    buf.put_u32(0); // fetch all rows

    stream.write_all(&buf).await.unwrap();

    // Sync
    buf.clear();
    buf.put_u8(b'S');
    buf.put_u32(4);

    stream.write_all(&buf).await.unwrap();
    stream.flush().await.unwrap();

    // Read responses
    let n = stream.read(&mut response).await.unwrap();
    println!("\nReceived {} bytes of response", n);

    let mut pos = 0;
    while pos < n {
        let msg_type = response[pos] as char;
        let msg_len = u32::from_be_bytes([
            response[pos + 1],
            response[pos + 2],
            response[pos + 3],
            response[pos + 4],
        ]) as usize;
        println!(
            "Message type: {} ({}), length: {}",
            msg_type, response[pos], msg_len
        );

        match response[pos] {
            b'1' => println!("  ParseComplete"),
            b'2' => println!("  BindComplete"),
            b'D' => {
                // Data row
                let field_count = u16::from_be_bytes([response[pos + 5], response[pos + 6]]);
                println!("  DataRow with {} fields", field_count);
            }
            b'C' => {
                // Command complete
                let tag_end = response[pos + 5..pos + msg_len + 1]
                    .iter()
                    .position(|&b| b == 0)
                    .unwrap_or(msg_len - 4);
                let tag = std::str::from_utf8(&response[pos + 5..pos + 5 + tag_end]).unwrap();
                println!("  CommandComplete: {}", tag);
            }
            b'E' => {
                // Error
                println!("  Error message");
                let mut err_pos = pos + 5;
                while err_pos < pos + msg_len + 1 && response[err_pos] != 0 {
                    let field_type = response[err_pos] as char;
                    err_pos += 1;
                    let field_end = response[err_pos..pos + msg_len + 1]
                        .iter()
                        .position(|&b| b == 0)
                        .unwrap_or(msg_len - err_pos + pos);
                    let field_value =
                        std::str::from_utf8(&response[err_pos..err_pos + field_end]).unwrap_or("?");
                    println!("    {}: {}", field_type, field_value);
                    err_pos += field_end + 1;
                }
            }
            b'Z' => println!("  ReadyForQuery"),
            _ => {}
        }

        pos += 1 + msg_len;
    }

    // Clean up
    server.abort();
}
