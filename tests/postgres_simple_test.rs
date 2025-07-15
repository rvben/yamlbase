use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use yamlbase::config::{Config, Protocol};
use yamlbase::database::{Column, Database, Storage, Table, Value};
use yamlbase::protocol::Connection;
use yamlbase::yaml::schema::SqlType;

#[tokio::test]
async fn test_postgres_simple_protocol() {
    let mut db = Database::new("test_db".to_string());

    // Create a test table
    let columns = vec![
        Column {
            name: "id".to_string(),
            sql_type: SqlType::Integer,
            primary_key: true,
            nullable: false,
            unique: true,
            default: None,
            references: None,
        },
        Column {
            name: "name".to_string(),
            sql_type: SqlType::Text,
            primary_key: false,
            nullable: false,
            unique: false,
            default: None,
            references: None,
        },
    ];

    let mut table = Table::new("users".to_string(), columns);

    // Insert test data
    table
        .insert_row(vec![Value::Integer(1), Value::Text("Alice".to_string())])
        .unwrap();

    table
        .insert_row(vec![Value::Integer(2), Value::Text("Bob".to_string())])
        .unwrap();

    db.add_table(table).unwrap();

    // Create storage and config
    let storage = Arc::new(Storage::new(db));
    let config = Arc::new(Config {
        file: PathBuf::from("test.yaml"),
        port: Some(0), // Let OS assign port
        bind_address: "127.0.0.1".to_string(),
        protocol: Protocol::Postgres,
        username: "yamlbase".to_string(),
        password: "password".to_string(),
        verbose: false,
        hot_reload: false,
        log_level: "info".to_string(),
        database: Some("test_db".to_string()),
    });

    // Start server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server_storage = storage.clone();
    let server_config = config.clone();

    // Spawn server task
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let connection = Connection::new(server_config.clone(), server_storage.clone());
            tokio::spawn(async move {
                if let Err(e) = connection.handle(stream).await {
                    eprintln!("Connection error: {}", e);
                }
            });
        }
    });

    // Give server time to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Connect and test
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // Send startup message
    use tokio::io::AsyncWriteExt;
    let mut startup = Vec::new();
    startup.extend(&16u32.to_be_bytes()); // length (will update)
    startup.extend(&196608u32.to_be_bytes()); // protocol version
    startup.extend(b"user\0yamlbase\0");
    startup.extend(b"database\0test_db\0");
    startup.push(0);

    // Update length
    let len = startup.len() as u32;
    startup[0..4].copy_from_slice(&len.to_be_bytes());

    stream.write_all(&startup).await.unwrap();

    // Read auth request
    use tokio::io::AsyncReadExt;
    let mut buf = vec![0u8; 1024];
    let n = stream.read(&mut buf).await.unwrap();

    // Check for auth request
    assert!(n > 0);
    assert_eq!(buf[0], b'R'); // Auth message

    // Send password
    let mut password_msg = Vec::new();
    password_msg.push(b'p');
    password_msg.extend(&13u32.to_be_bytes()); // length: 4 (length field) + 9 ("password\0")
    password_msg.extend(b"password\0");

    stream.write_all(&password_msg).await.unwrap();

    // Read auth response and ready for query
    // The server sends multiple messages after authentication:
    // - Auth OK (R)
    // - Backend Key Data (K)
    // - Parameter Status messages (S)
    // - ReadyForQuery (Z)
    // We need to read enough data to get all of them

    let mut total_read = 0;
    let mut found_ready = false;
    buf.resize(4096, 0); // Increase buffer size

    // Keep reading until we find ReadyForQuery
    while total_read < buf.len() && !found_ready {
        let n = stream.read(&mut buf[total_read..]).await.unwrap();
        if n == 0 {
            break;
        }

        // Look for ReadyForQuery message (Z)
        for i in 0..total_read + n {
            if buf[i] == b'Z' && i + 5 <= total_read + n {
                // Check if this is a valid ReadyForQuery message
                let len = u32::from_be_bytes([buf[i + 1], buf[i + 2], buf[i + 3], buf[i + 4]]);
                if len == 5 && i + 6 <= total_read + n && buf[i + 5] == b'I' {
                    found_ready = true;
                    break;
                }
            }
        }

        total_read += n;
    }

    assert!(found_ready, "Server should send ready for query");

    // Send simple query
    let query = "SELECT * FROM users WHERE id = 1";
    let mut query_msg = Vec::new();
    query_msg.push(b'Q');
    query_msg.extend(&((query.len() + 5) as u32).to_be_bytes());
    query_msg.extend(query.as_bytes());
    query_msg.push(0);

    stream.write_all(&query_msg).await.unwrap();

    // Read response
    buf.clear();
    buf.resize(4096, 0);
    let n = stream.read(&mut buf).await.unwrap();
    assert!(n > 0);

    // Verify we got data back
    let response = &buf[..n];

    // Look for row description ('T')
    let row_desc_pos = response.iter().position(|&b| b == b'T');
    assert!(row_desc_pos.is_some(), "Should receive row description");

    // Look for data row ('D')
    let data_pos = response.iter().position(|&b| b == b'D');
    assert!(data_pos.is_some(), "Should receive data row");

    // Look for command complete ('C')
    let complete_pos = response.iter().position(|&b| b == b'C');
    assert!(complete_pos.is_some(), "Should receive command complete");

    // Send terminate
    let terminate = vec![b'X', 0, 0, 0, 4];
    stream.write_all(&terminate).await.unwrap();
}
