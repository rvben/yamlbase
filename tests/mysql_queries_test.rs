use std::net::TcpStream;
use std::io::{Read, Write};
use std::time::Duration;

#[test]
fn test_mysql_various_queries() {
    let mut server = start_test_server();
    std::thread::sleep(Duration::from_secs(2));
    
    let result = std::panic::catch_unwind(|| {
        let mut stream = connect_and_auth();
        
        // Test SELECT without FROM
        test_query(&mut stream, "SELECT 1", vec!["1"]);
        test_query(&mut stream, "SELECT 1 + 1", vec!["2"]);
        test_query(&mut stream, "SELECT 5 - 3", vec!["2"]);
        test_query(&mut stream, "SELECT 3 * 4", vec!["12"]);
        test_query(&mut stream, "SELECT 10 / 2", vec!["5"]);
        test_query(&mut stream, "SELECT 'hello'", vec!["hello"]);
        test_query(&mut stream, "SELECT 1, 2, 3", vec!["1", "2", "3"]);
        test_query(&mut stream, "SELECT 1 AS num", vec!["1"]);
        test_query(&mut stream, "SELECT -5", vec!["-5"]);
        test_query(&mut stream, "SELECT true", vec!["true"]);
        test_query(&mut stream, "SELECT false", vec!["false"]);
        test_query(&mut stream, "SELECT null", vec!["NULL"]);
        
        // Test SELECT with FROM
        test_query(&mut stream, "SELECT * FROM users WHERE id = 1", vec!["1", "alice", "alice@example.com", "2024-01-15 10:30:00"]);
        test_query(&mut stream, "SELECT username FROM users", vec!["alice", "bob"]);
        test_query(&mut stream, "SELECT id, username FROM users ORDER BY id DESC", vec!["2", "bob", "1", "alice"]);
        
        // Test PING command
        test_ping(&mut stream);
    });
    
    server.kill().expect("Failed to kill server");
    
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

#[test]
fn test_mysql_set_commands() {
    let mut server = start_test_server();
    std::thread::sleep(Duration::from_secs(2));
    
    let result = std::panic::catch_unwind(|| {
        let mut stream = connect_and_auth();
        
        // Test SET NAMES command (should succeed)
        test_ok_response(&mut stream, "SET NAMES 'utf8mb4' COLLATE 'utf8mb4_0900_ai_ci'");
        test_ok_response(&mut stream, "SET NAMES utf8mb4");
        test_ok_response(&mut stream, "SET NAMES 'utf8'");
        
        // Test SET CHARACTER SET command
        test_ok_response(&mut stream, "SET CHARACTER SET utf8mb4");
        test_ok_response(&mut stream, "SET CHARACTER SET 'utf8'");
        
        // Test other SET commands
        test_ok_response(&mut stream, "SET SESSION sql_mode='TRADITIONAL'");
        test_ok_response(&mut stream, "SET autocommit=1");
        
        // Verify that queries still work after SET commands
        test_query(&mut stream, "SELECT 1", vec!["1"]);
        test_query(&mut stream, "SELECT username FROM users WHERE id = 1", vec!["alice"]);
    });
    
    server.kill().expect("Failed to kill server");
    
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

#[test]
fn test_mysql_set_names_edge_cases() {
    let mut server = start_test_server();
    std::thread::sleep(Duration::from_secs(2));
    
    let result = std::panic::catch_unwind(|| {
        let mut stream = connect_and_auth();
        
        // Test case variations
        test_ok_response(&mut stream, "set names utf8mb4");  // lowercase
        test_ok_response(&mut stream, "SET NAMES UTF8MB4");  // uppercase charset
        test_ok_response(&mut stream, "Set Names Utf8");     // mixed case
        
        // Test with various character sets
        test_ok_response(&mut stream, "SET NAMES latin1");
        test_ok_response(&mut stream, "SET NAMES ascii");
        test_ok_response(&mut stream, "SET NAMES big5");
        test_ok_response(&mut stream, "SET NAMES cp1251");
        
        // Test with backticks and double quotes
        test_ok_response(&mut stream, "SET NAMES `utf8mb4`");
        test_ok_response(&mut stream, "SET NAMES \"utf8\"");
        
        // Test multiple SET commands in sequence
        test_ok_response(&mut stream, "SET NAMES utf8");
        test_ok_response(&mut stream, "SET NAMES utf8mb4");
        test_ok_response(&mut stream, "SET NAMES latin1");
        
        // Test with extra spaces
        test_ok_response(&mut stream, "SET  NAMES   utf8mb4");
        test_ok_response(&mut stream, "SET NAMES utf8mb4 ");
        test_ok_response(&mut stream, " SET NAMES utf8mb4");
        
        // Test with complex COLLATE specifications
        test_ok_response(&mut stream, "SET NAMES 'utf8mb4' COLLATE 'utf8mb4_unicode_ci'");
        test_ok_response(&mut stream, "SET NAMES utf8 COLLATE utf8_general_ci");
        test_ok_response(&mut stream, "SET NAMES 'latin1' COLLATE 'latin1_swedish_ci'");
        
        // Ensure database still works after edge cases
        test_query(&mut stream, "SELECT id FROM users ORDER BY id", vec!["1", "2"]);
    });
    
    server.kill().expect("Failed to kill server");
    
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

#[test]
fn test_mysql_set_session_variables() {
    let mut server = start_test_server();
    std::thread::sleep(Duration::from_secs(2));
    
    let result = std::panic::catch_unwind(|| {
        let mut stream = connect_and_auth();
        
        // Test various MySQL session variables that clients commonly set
        test_ok_response(&mut stream, "SET time_zone = '+00:00'");
        test_ok_response(&mut stream, "SET sql_mode = 'STRICT_TRANS_TABLES'");
        test_ok_response(&mut stream, "SET autocommit = 0");
        test_ok_response(&mut stream, "SET SESSION sql_safe_updates = 1");
        test_ok_response(&mut stream, "SET @@session.sql_mode = 'TRADITIONAL'");
        test_ok_response(&mut stream, "SET SESSION transaction_isolation = 'READ-COMMITTED'");
        test_ok_response(&mut stream, "SET GLOBAL max_connections = 1000");
        test_ok_response(&mut stream, "SET @@global.max_connections = 500");
        
        // Test with various formats
        test_ok_response(&mut stream, "SET @@sql_mode='NO_ENGINE_SUBSTITUTION'");
        test_ok_response(&mut stream, "SET SESSION autocommit=ON");
        test_ok_response(&mut stream, "SET autocommit=OFF");
        
        // Verify queries still work
        test_query(&mut stream, "SELECT 'test'", vec!["test"]);
    });
    
    server.kill().expect("Failed to kill server");
    
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

#[test]
fn test_mysql_error_handling() {
    let mut server = start_test_server();
    std::thread::sleep(Duration::from_secs(2));
    
    let result = std::panic::catch_unwind(|| {
        let mut stream = connect_and_auth();
        
        // Test syntax error
        test_error(&mut stream, "INVALID SQL", "Syntax error");
        
        // Test table not found
        test_error(&mut stream, "SELECT * FROM nonexistent", "Table 'nonexistent' not found");
        
        // Test column not found
        test_error(&mut stream, "SELECT invalid_col FROM users", "Column 'invalid_col' not found");
        
        // Test division by zero
        test_error(&mut stream, "SELECT 1 / 0", "Division by zero");
    });
    
    server.kill().expect("Failed to kill server");
    
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

#[test]
fn test_mysql_protocol_commands() {
    let mut server = start_test_server();
    std::thread::sleep(Duration::from_secs(2));
    
    let result = std::panic::catch_unwind(|| {
        let mut stream = connect_and_auth();
        
        // Test COM_INIT_DB (USE database)
        test_init_db(&mut stream, "test_db");
        test_init_db(&mut stream, "another_db");
        test_init_db(&mut stream, "");  // Empty database name
        
        // Test multiple PINGs
        for _ in 0..5 {
            test_ping(&mut stream);
        }
        
        // Test queries after switching database
        test_query(&mut stream, "SELECT 42", vec!["42"]);
        
        // Test edge case: very long database name in COM_INIT_DB
        let long_db_name = "a".repeat(100);
        test_init_db(&mut stream, &long_db_name);
    });
    
    server.kill().expect("Failed to kill server");
    
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

#[test]
fn test_mysql_query_edge_cases() {
    let mut server = start_test_server();
    std::thread::sleep(Duration::from_secs(2));
    
    let result = std::panic::catch_unwind(|| {
        let mut stream = connect_and_auth();
        
        // Test empty query
        test_error(&mut stream, "", "Syntax error");
        
        // Test query with only whitespace
        test_error(&mut stream, "   \t\n  ", "Syntax error");
        
        // Test query with comments
        test_query(&mut stream, "SELECT 1 -- this is a comment", vec!["1"]);
        test_query(&mut stream, "/* comment */ SELECT 2", vec!["2"]);
        test_query(&mut stream, "SELECT /* inline */ 3", vec!["3"]);
        
        // Test queries with special characters in strings
        test_query(&mut stream, "SELECT 'hello''world'", vec!["hello'world"]);
        test_query(&mut stream, "SELECT 'line1\nline2'", vec!["line1\nline2"]);
        test_query(&mut stream, "SELECT 'tab\there'", vec!["tab\there"]);
        
        // Test queries with backticks (MySQL-style identifiers)
        test_query(&mut stream, "SELECT `username` FROM `users` WHERE `id` = 1", vec!["alice"]);
        
        // Test case sensitivity
        test_query(&mut stream, "select USERNAME from USERS where ID = 1", vec!["alice"]);
        test_query(&mut stream, "SeLeCt UsErNaMe FrOm UsErS wHeRe Id = 1", vec!["alice"]);
        
        // Test very long query
        let long_condition = (0..100).map(|_| "1=1").collect::<Vec<_>>().join(" AND ");
        let long_query = format!("SELECT 1 WHERE {}", long_condition);
        test_query(&mut stream, &long_query, vec!["1"]);
    });
    
    server.kill().expect("Failed to kill server");
    
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

#[test]
fn test_mysql_unknown_commands() {
    let mut server = start_test_server();
    std::thread::sleep(Duration::from_secs(2));
    
    let result = std::panic::catch_unwind(|| {
        let mut stream = connect_and_auth();
        
        // Test various unknown command bytes
        test_unknown_command(&mut stream, 0x04); // COM_FIELD_LIST
        test_unknown_command(&mut stream, 0x05); // COM_CREATE_DB
        test_unknown_command(&mut stream, 0x06); // COM_DROP_DB
        test_unknown_command(&mut stream, 0x07); // COM_REFRESH
        test_unknown_command(&mut stream, 0x08); // COM_SHUTDOWN
        test_unknown_command(&mut stream, 0x09); // COM_STATISTICS
        test_unknown_command(&mut stream, 0x0a); // COM_PROCESS_INFO
        test_unknown_command(&mut stream, 0x0c); // COM_PROCESS_KILL
        test_unknown_command(&mut stream, 0x0d); // COM_DEBUG
        test_unknown_command(&mut stream, 0x10); // COM_CHANGE_USER
        test_unknown_command(&mut stream, 0x1a); // COM_STMT_EXECUTE
        test_unknown_command(&mut stream, 0xff); // Invalid command
        
        // Verify server still works after unknown commands
        test_ping(&mut stream);
        test_query(&mut stream, "SELECT 'still working'", vec!["still working"]);
    });
    
    server.kill().expect("Failed to kill server");
    
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

#[test]
fn test_mysql_system_variables() {
    let mut server = start_test_server();
    std::thread::sleep(Duration::from_secs(2));
    
    let result = std::panic::catch_unwind(|| {
        let mut stream = connect_and_auth();
        
        // Test @@version variations
        test_query(&mut stream, "SELECT @@version", vec!["8.0.35-yamlbase"]);
        test_query(&mut stream, "SELECT @@VERSION", vec!["8.0.35-yamlbase"]);
        test_query(&mut stream, "SELECT @@version_comment", vec!["1"]);
        
        // Test other system variables (should return "1" as default)
        test_query(&mut stream, "SELECT @@sql_mode", vec!["1"]);
        test_query(&mut stream, "SELECT @@autocommit", vec!["1"]);
        test_query(&mut stream, "SELECT @@max_connections", vec!["1"]);
        test_query(&mut stream, "SELECT @@character_set_client", vec!["1"]);
        test_query(&mut stream, "SELECT @@character_set_connection", vec!["1"]);
        test_query(&mut stream, "SELECT @@character_set_results", vec!["1"]);
        
        // Test session and global prefixes
        test_query(&mut stream, "SELECT @@session.autocommit", vec!["1"]);
        test_query(&mut stream, "SELECT @@global.max_connections", vec!["1"]);
        test_query(&mut stream, "SELECT @@SESSION.sql_mode", vec!["1"]);
        test_query(&mut stream, "SELECT @@GLOBAL.version", vec!["8.0.35-yamlbase"]);
        
        // Test multiple system variables in one query
        test_query(&mut stream, "SELECT @@version, @@sql_mode", vec!["8.0.35-yamlbase", "1"]);
        
        // Test system variables mixed with regular queries
        test_query(&mut stream, "SELECT @@version, 42, 'hello'", vec!["8.0.35-yamlbase", "42", "hello"]);
    });
    
    server.kill().expect("Failed to kill server");
    
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

// Helper functions

fn start_test_server() -> std::process::Child {
    std::process::Command::new("cargo")
        .args(&["run", "--", "-f", "examples/database_with_auth.yaml", "--protocol", "mysql", "-p", "13308"])
        .spawn()
        .expect("Failed to start server")
}

fn connect_and_auth() -> TcpStream {
    let mut stream = TcpStream::connect("127.0.0.1:13308")
        .expect("Failed to connect to server");
    
    // Read handshake
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).expect("Failed to read header");
    let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
    let sequence = header[3];
    
    let mut handshake = vec![0u8; length as usize];
    stream.read_exact(&mut handshake).expect("Failed to read handshake");
    
    // Extract auth data
    let pos = handshake.iter().position(|&b| b == 0).unwrap() + 1 + 4;
    let auth_data_1 = &handshake[pos..pos+8];
    let auth_data_2 = &handshake[pos+8+1+2+1+2+2+1+10..pos+8+1+2+1+2+2+1+10+12];
    let mut auth_data = auth_data_1.to_vec();
    auth_data.extend_from_slice(auth_data_2);
    
    // Send auth response
    let mut response = Vec::new();
    response.extend(&0x000fa685u32.to_le_bytes()); // capabilities
    response.extend(&16777216u32.to_le_bytes());   // max packet
    response.push(33);                              // charset
    response.extend(&[0u8; 23]);                    // reserved
    response.extend(b"dbadmin\x00");                // username
    
    let auth_response = compute_auth_response("securepass123", &auth_data);
    response.push(auth_response.len() as u8);
    response.extend(&auth_response);
    
    let mut packet = Vec::new();
    packet.extend(&(response.len() as u32).to_le_bytes()[..3]);
    packet.push(sequence + 1);
    packet.extend(&response);
    stream.write_all(&packet).expect("Failed to send handshake response");
    
    // Read auth result
    stream.read_exact(&mut header).expect("Failed to read auth response header");
    let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
    let mut auth_result = vec![0u8; length as usize];
    stream.read_exact(&mut auth_result).expect("Failed to read auth response");
    
    assert_eq!(auth_result[0], 0x00, "Authentication should succeed");
    
    stream
}

fn test_query(stream: &mut TcpStream, query: &str, expected_values: Vec<&str>) {
    // Send query
    let query_packet = format!("\x03{}", query);
    let mut packet = Vec::new();
    packet.extend(&(query_packet.len() as u32).to_le_bytes()[..3]);
    packet.push(0); // sequence
    packet.extend(query_packet.as_bytes());
    stream.write_all(&packet).expect("Failed to send query");
    
    // Read response
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).expect("Failed to read response header");
    let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
    let mut response = vec![0u8; length as usize];
    stream.read_exact(&mut response).expect("Failed to read response");
    
    if response[0] == 0xff {
        // Error packet
        let error_code = u16::from_le_bytes([response[1], response[2]]);
        let error_msg = String::from_utf8_lossy(&response[9..]);
        panic!("Query '{}' failed with error {}: {}", query, error_code, error_msg);
    }
    
    // Column count
    let column_count = response[0] as usize;
    
    // Read column definitions
    for _ in 0..column_count {
        stream.read_exact(&mut header).expect("Failed to read column header");
        let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
        let mut _col_def = vec![0u8; length as usize];
        stream.read_exact(&mut _col_def).expect("Failed to read column def");
    }
    
    // Read rows and collect values
    let mut values = Vec::new();
    loop {
        stream.read_exact(&mut header).expect("Failed to read row header");
        let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
        let mut row_data = vec![0u8; length as usize];
        stream.read_exact(&mut row_data).expect("Failed to read row data");
        
        if row_data[0] == 0x00 && length < 10 {
            // OK packet - end of results
            break;
        }
        
        // Parse row values (simplified - assumes all are length-prefixed strings)
        let mut pos = 0;
        while pos < row_data.len() {
            if row_data[pos] == 0xfb {
                // NULL value
                values.push("NULL".to_string());
                pos += 1;
            } else if row_data[pos] < 251 {
                let len = row_data[pos] as usize;
                let value = String::from_utf8_lossy(&row_data[pos+1..pos+1+len]).to_string();
                values.push(value);
                pos += 1 + len;
            } else {
                panic!("Unsupported length encoding");
            }
        }
    }
    
    assert_eq!(values.len(), expected_values.len(), 
        "Query '{}' returned {} values, expected {}", query, values.len(), expected_values.len());
    
    for (i, (actual, expected)) in values.iter().zip(expected_values.iter()).enumerate() {
        assert_eq!(actual, expected, 
            "Query '{}' value {} mismatch: got '{}', expected '{}'", query, i, actual, expected);
    }
}

fn test_error(stream: &mut TcpStream, query: &str, expected_error: &str) {
    // Send query
    let query_packet = format!("\x03{}", query);
    let mut packet = Vec::new();
    packet.extend(&(query_packet.len() as u32).to_le_bytes()[..3]);
    packet.push(0); // sequence
    packet.extend(query_packet.as_bytes());
    stream.write_all(&packet).expect("Failed to send query");
    
    // Read response
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).expect("Failed to read response header");
    let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
    let mut response = vec![0u8; length as usize];
    stream.read_exact(&mut response).expect("Failed to read response");
    
    assert_eq!(response[0], 0xff, "Expected error packet for query '{}'", query);
    
    let error_msg = String::from_utf8_lossy(&response[9..]);
    assert!(error_msg.contains(expected_error), 
        "Query '{}' error message '{}' should contain '{}'", query, error_msg, expected_error);
}

fn test_ping(stream: &mut TcpStream) {
    // Send PING command
    let ping_packet = b"\x0e"; // COM_PING
    let mut packet = Vec::new();
    packet.extend(&(ping_packet.len() as u32).to_le_bytes()[..3]);
    packet.push(0); // sequence
    packet.extend(ping_packet);
    stream.write_all(&packet).expect("Failed to send ping");
    
    // Read response
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).expect("Failed to read ping response header");
    let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
    let mut response = vec![0u8; length as usize];
    stream.read_exact(&mut response).expect("Failed to read ping response");
    
    assert_eq!(response[0], 0x00, "PING should return OK packet");
}

fn test_init_db(stream: &mut TcpStream, database: &str) {
    // Send COM_INIT_DB command
    let mut init_db_packet = vec![0x02]; // COM_INIT_DB
    init_db_packet.extend(database.as_bytes());
    
    let mut packet = Vec::new();
    packet.extend(&(init_db_packet.len() as u32).to_le_bytes()[..3]);
    packet.push(0); // sequence
    packet.extend(&init_db_packet);
    stream.write_all(&packet).expect("Failed to send init_db");
    
    // Read response
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).expect("Failed to read init_db response header");
    let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
    let mut response = vec![0u8; length as usize];
    stream.read_exact(&mut response).expect("Failed to read init_db response");
    
    assert_eq!(response[0], 0x00, "COM_INIT_DB should return OK packet");
}

fn test_unknown_command(stream: &mut TcpStream, command: u8) {
    // Send unknown command
    let cmd_packet = vec![command];
    let mut packet = Vec::new();
    packet.extend(&(cmd_packet.len() as u32).to_le_bytes()[..3]);
    packet.push(0); // sequence
    packet.extend(&cmd_packet);
    stream.write_all(&packet).expect("Failed to send unknown command");
    
    // Read response - should be error
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).expect("Failed to read response header");
    let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
    let mut response = vec![0u8; length as usize];
    stream.read_exact(&mut response).expect("Failed to read response");
    
    assert_eq!(response[0], 0xff, "Unknown command should return error packet");
    let error_msg = String::from_utf8_lossy(&response[9..]);
    assert!(error_msg.contains("Unknown command"), 
        "Error message '{}' should contain 'Unknown command'", error_msg);
}

fn test_ok_response(stream: &mut TcpStream, query: &str) {
    // Send query
    let query_packet = format!("\x03{}", query);
    let mut packet = Vec::new();
    packet.extend(&(query_packet.len() as u32).to_le_bytes()[..3]);
    packet.push(0); // sequence
    packet.extend(query_packet.as_bytes());
    stream.write_all(&packet).expect("Failed to send query");
    
    // Read response
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).expect("Failed to read response header");
    let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
    let mut response = vec![0u8; length as usize];
    stream.read_exact(&mut response).expect("Failed to read response");
    
    if response[0] == 0xff {
        // Error packet
        let error_code = u16::from_le_bytes([response[1], response[2]]);
        let error_msg = String::from_utf8_lossy(&response[9..]);
        panic!("Query '{}' failed with error {}: {}", query, error_code, error_msg);
    }
    
    assert_eq!(response[0], 0x00, "Query '{}' should return OK packet", query);
}

fn compute_auth_response(password: &str, auth_data: &[u8]) -> Vec<u8> {
    use sha1::{Digest, Sha1};
    
    if password.is_empty() {
        return Vec::new();
    }
    
    let mut hasher = Sha1::new();
    hasher.update(password.as_bytes());
    let stage1 = hasher.finalize();
    
    let mut hasher = Sha1::new();
    hasher.update(&stage1);
    let stage2 = hasher.finalize();
    
    let mut hasher = Sha1::new();
    hasher.update(auth_data);
    hasher.update(&stage2);
    let result = hasher.finalize();
    
    stage1.iter()
        .zip(result.iter())
        .map(|(a, b)| a ^ b)
        .collect()
}