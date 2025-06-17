use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test]
async fn test_postgres_extended_protocol_flow() {
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
      age: "INTEGER"
    data:
      - id: 1
        name: "Alice"
        age: 30
      - id: 2
        name: "Bob"
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
    let server = tokio::spawn(async move {
        let _ = tokio::process::Command::new("cargo")
            .args(&[
                "run",
                "--",
                "-f",
                temp_file.path().to_str().unwrap(),
                "--protocol",
                "postgres",
                "-p",
                &port.to_string(),
                "--log-level",
                "debug",
            ])
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
    println!("Auth request received: {} bytes", n);

    // Send password
    buf.clear();
    buf.put_u8(b'p');
    let password = "password";
    buf.put_u32(4 + password.len() as u32 + 1);
    buf.put_slice(password.as_bytes());
    buf.put_u8(0);

    stream.write_all(&buf).await.unwrap();

    // Read auth response and ReadyForQuery
    let n = stream.read(&mut response).await.unwrap();
    println!("Auth response received: {} bytes", n);

    // Extended protocol flow: Parse -> Describe -> Bind -> Execute -> Sync

    // 1. Send Parse message
    buf.clear();
    buf.put_u8(b'P');
    let stmt_name = "stmt1";
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

    println!("Sending Parse message");
    stream.write_all(&buf).await.unwrap();

    // 2. Send Describe message
    buf.clear();
    buf.put_u8(b'D');
    buf.put_u32((4 + 1 + stmt_name.len() + 1) as u32);
    buf.put_u8(b'S'); // Describe statement
    buf.put_slice(stmt_name.as_bytes());
    buf.put_u8(0);

    println!("Sending Describe message");
    stream.write_all(&buf).await.unwrap();

    // 3. Send Sync to trigger responses
    buf.clear();
    buf.put_u8(b'S');
    buf.put_u32(4);

    println!("Sending Sync message");
    stream.write_all(&buf).await.unwrap();
    stream.flush().await.unwrap();

    // Read responses
    println!("Waiting for responses...");
    match tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        stream.read(&mut response),
    )
    .await
    {
        Ok(Ok(n)) => {
            println!("Received {} bytes", n);
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
                    "Message: {} ({}), length: {}",
                    msg_type, response[pos], msg_len
                );
                pos += 1 + msg_len;
            }
        }
        Ok(Err(e)) => panic!("Read error: {}", e),
        Err(_) => panic!("Timeout waiting for responses"),
    }

    // Clean up
    server.abort();
}
