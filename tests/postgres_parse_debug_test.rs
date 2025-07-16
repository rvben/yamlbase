#![allow(clippy::uninlined_format_args)]

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

#[tokio::test]
async fn test_postgres_parse_message_directly() {
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
      name: "TEXT"
    data:
      - id: 1
        name: "test"
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

    // Connect to the server
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

    // Read authentication request
    let mut response = vec![0u8; 1024];
    let n = stream.read(&mut response).await.unwrap();
    println!("Auth request: {:?}", &response[..n]);

    // Send password
    buf.clear();
    buf.put_u8(b'p');
    let password = "password";
    buf.put_u32(4 + password.len() as u32 + 1);
    buf.put_slice(password.as_bytes());
    buf.put_u8(0);

    stream.write_all(&buf).await.unwrap();

    // Read auth response and consume all messages until ReadyForQuery
    let mut total_read = 0;
    let mut found_ready = false;

    // Keep reading until we find ReadyForQuery
    while total_read < response.len() && !found_ready {
        let n = stream.read(&mut response[total_read..]).await.unwrap();
        if n == 0 {
            break;
        }

        // Look for ReadyForQuery message (Z)
        for i in 0..total_read + n {
            if response[i] == b'Z' && i + 5 <= total_read + n {
                // Check if this is a valid ReadyForQuery message
                let len = u32::from_be_bytes([
                    response[i + 1],
                    response[i + 2],
                    response[i + 3],
                    response[i + 4],
                ]);
                if len == 5 && i + 6 <= total_read + n && response[i + 5] == b'I' {
                    found_ready = true;
                    break;
                }
            }
        }

        total_read += n;
    }

    println!(
        "Auth response and parameter status messages: {:?}",
        &response[..total_read]
    );
    assert!(found_ready, "Should receive ReadyForQuery message");

    // Now send a Parse message
    buf.clear();
    buf.put_u8(b'P');
    let stmt_name = "";
    let query = "SELECT * FROM users WHERE id = $1";
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

    println!("Sending Parse message: {:?}", &buf[..]);
    stream.write_all(&buf).await.unwrap();

    // Send Sync message to trigger response
    buf.clear();
    buf.put_u8(b'S');
    buf.put_u32(4); // Length
    stream.write_all(&buf).await.unwrap();
    stream.flush().await.unwrap();

    // Read response which should contain ParseComplete
    response.clear();
    response.resize(1024, 0);
    println!("Waiting for ParseComplete...");

    match tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        stream.read(&mut response),
    )
    .await
    {
        Ok(Ok(n)) => {
            println!("Received {} bytes: {:?}", n, &response[..n]);
            assert!(n > 0, "Expected response");

            // Look for ParseComplete message ('1')
            let mut found_parse_complete = false;
            for i in 0..n {
                if response[i] == b'1' && i + 5 <= n {
                    let len = u32::from_be_bytes([
                        response[i + 1],
                        response[i + 2],
                        response[i + 3],
                        response[i + 4],
                    ]);
                    if len == 4 {
                        found_parse_complete = true;
                        break;
                    }
                }
            }
            assert!(found_parse_complete, "Expected ParseComplete (1) message");
        }
        Ok(Err(e)) => panic!("Read error: {}", e),
        Err(_) => panic!("Timeout waiting for ParseComplete"),
    }

    // Clean up
    server.abort();
}
