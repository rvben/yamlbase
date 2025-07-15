#![allow(clippy::uninlined_format_args)]

use std::time::Duration;
use tokio::time::timeout;
use tempfile::NamedTempFile;
use yamlbase::{Config, Server};
use yamlbase::config::Protocol;

/// This test ensures that the server doesn't use blocking operations in async context
/// which was the cause of the panic in issue #1
#[tokio::test]
async fn test_no_blocking_in_async_context() {
    let yaml = r#"
database:
  name: "test_db"

tables:
  test:
    columns:
      id: "INTEGER PRIMARY KEY"
"#;

    // Create temporary YAML file
    let mut temp_file = NamedTempFile::new().unwrap();
    std::io::Write::write_all(&mut temp_file, yaml.as_bytes()).unwrap();
    let yaml_path = temp_file.path().to_str().unwrap().to_string();
    
    // Start server in background task
    let server_handle = tokio::spawn(async move {
        let config = Config {
            file: yaml_path.into(),
            port: Some(25432), // Use different port to avoid conflicts
            bind_address: "127.0.0.1".to_string(),
            protocol: Protocol::Postgres,
            username: "admin".to_string(),
            password: "password".to_string(),
            hot_reload: false,
            verbose: false,
            log_level: "error".to_string(),
            database: None,
        };
        
        let server = Server::new(config).await.unwrap();
        // This should not panic with "Cannot drop a runtime in a context where blocking is not allowed"
        server.run().await
    });
    
    // Give server time to start
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // Try to connect - this would trigger the blocking_read panic in the old code
    let connect_result = tokio::time::timeout(
        Duration::from_secs(2),
        async {
            tokio::net::TcpStream::connect("127.0.0.1:25432").await
        }
    ).await;
    
    // We should be able to connect without the server panicking
    assert!(connect_result.is_ok(), "Should be able to connect to server");
    assert!(connect_result.unwrap().is_ok(), "Connection should succeed");
    
    // Abort the server task
    server_handle.abort();
}

/// Test that both PostgreSQL and MySQL protocols handle multiple concurrent connections
/// without blocking issues
#[tokio::test] 
async fn test_concurrent_connections_no_blocking() {
    for protocol in &["postgres", "mysql"] {
        let yaml = r#"
database:
  name: "test_db"

tables:
  test:
    columns:
      id: "INTEGER PRIMARY KEY"
"#;

        let mut temp_file = NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut temp_file, yaml.as_bytes()).unwrap();
        let yaml_path = temp_file.path().to_str().unwrap().to_string();
        
        let port = if *protocol == "postgres" { 25433 } else { 23306 };
        let protocol_enum = if *protocol == "postgres" {
            Protocol::Postgres
        } else {
            Protocol::Mysql
        };
        
        // Start server
        let server_handle = tokio::spawn(async move {
            let config = Config {
                file: yaml_path.into(),
                port: Some(port),
                bind_address: "127.0.0.1".to_string(),
                protocol: protocol_enum,
                username: "admin".to_string(),
                password: "password".to_string(),
                hot_reload: false,
                verbose: false,
                log_level: "error".to_string(),
                database: None,
            };
            
            let server = Server::new(config).await.unwrap();
            server.run().await
        });
        
        // Wait for server to start
        tokio::time::sleep(Duration::from_millis(500)).await;
        
        // Create multiple concurrent connections
        let mut handles = vec![];
        for _ in 0..5 {
            let handle = tokio::spawn(async move {
                timeout(
                    Duration::from_secs(1),
                    tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
                ).await
            });
            handles.push(handle);
        }
        
        // All connections should succeed without blocking
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok(), "Connection should not timeout");
            assert!(result.unwrap().is_ok(), "Connection should succeed");
        }
        
        server_handle.abort();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}