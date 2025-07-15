#![allow(clippy::uninlined_format_args)]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::time::Duration;

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

#[test]
fn test_mysql_connection_and_simple_query() {
    // Start server in background
    let (cmd, mut args) = get_yamlbase_command();
    args.extend(vec![
        "-f".to_string(),
        "examples/database_with_auth.yaml".to_string(),
        "--protocol".to_string(),
        "mysql".to_string(),
        "-p".to_string(),
        "13306".to_string(),
    ]);

    let mut server = std::process::Command::new(&cmd)
        .args(&args)
        .spawn()
        .expect("Failed to start server");

    // Wait for server to start
    std::thread::sleep(Duration::from_secs(2));

    let result = std::panic::catch_unwind(|| {
        // Connect to server
        let mut stream =
            TcpStream::connect("127.0.0.1:13306").expect("Failed to connect to server");

        // Read handshake packet
        let mut header = [0u8; 4];
        stream
            .read_exact(&mut header)
            .expect("Failed to read header");
        let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
        let _sequence = header[3];

        let mut handshake = vec![0u8; length as usize];
        stream
            .read_exact(&mut handshake)
            .expect("Failed to read handshake");

        // Extract auth data
        let pos = handshake.iter().position(|&b| b == 0).unwrap() + 1 + 4;
        let auth_data_1 = &handshake[pos..pos + 8];
        let auth_data_2 = &handshake
            [pos + 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10..pos + 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10 + 12];
        let mut auth_data = auth_data_1.to_vec();
        auth_data.extend_from_slice(auth_data_2);

        // Send handshake response
        let mut response = Vec::new();
        response.extend(&0x000fa685u32.to_le_bytes()); // capabilities
        response.extend(&16777216u32.to_le_bytes()); // max packet
        response.push(33); // charset
        response.extend(&[0u8; 23]); // reserved
        response.extend(b"dbadmin\x00"); // username

        // Compute auth response
        let auth_response = compute_auth_response("securepass123", &auth_data);
        response.push(auth_response.len() as u8);
        response.extend(&auth_response);

        // Write packet
        let mut packet = Vec::new();
        packet.extend(&(response.len() as u32).to_le_bytes()[..3]);
        packet.push(1); // sequence
        packet.extend(&response);
        stream
            .write_all(&packet)
            .expect("Failed to send handshake response");

        // Read auth result
        stream
            .read_exact(&mut header)
            .expect("Failed to read auth response header");
        let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
        let mut auth_result = vec![0u8; length as usize];
        stream
            .read_exact(&mut auth_result)
            .expect("Failed to read auth response");

        assert_eq!(auth_result[0], 0x00, "Authentication should succeed");

        // Send SELECT 1 query
        let query = b"\x03SELECT 1"; // COM_QUERY + query
        let mut packet = Vec::new();
        packet.extend(&(query.len() as u32).to_le_bytes()[..3]);
        packet.push(0); // sequence
        packet.extend(query);
        stream.write_all(&packet).expect("Failed to send query");

        // Read query response
        stream
            .read_exact(&mut header)
            .expect("Failed to read query response header");
        let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
        let mut response = vec![0u8; length as usize];
        stream
            .read_exact(&mut response)
            .expect("Failed to read query response");

        // Should get column count (1)
        assert_eq!(response[0], 1, "Should get 1 column");

        // Read column definition
        stream
            .read_exact(&mut header)
            .expect("Failed to read column def header");
        let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
        let mut _col_def = vec![0u8; length as usize];
        stream
            .read_exact(&mut _col_def)
            .expect("Failed to read column def");

        // Read EOF packet after column definitions
        stream
            .read_exact(&mut header)
            .expect("Failed to read EOF header after columns");
        let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
        let mut eof_packet = vec![0u8; length as usize];
        stream
            .read_exact(&mut eof_packet)
            .expect("Failed to read EOF packet");
        assert_eq!(eof_packet[0], 0xfe, "Should get EOF packet after columns");

        // Read row data
        stream
            .read_exact(&mut header)
            .expect("Failed to read row header");
        let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
        let mut row_data = vec![0u8; length as usize];
        stream
            .read_exact(&mut row_data)
            .expect("Failed to read row data");

        // Check row contains "1"
        assert_eq!(row_data[0], 1, "Length should be 1");
        assert_eq!(row_data[1], b'1', "Value should be '1'");

        // Read EOF packet (end of results)
        stream
            .read_exact(&mut header)
            .expect("Failed to read final EOF header");
        let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
        let mut final_eof = vec![0u8; length as usize];
        stream
            .read_exact(&mut final_eof)
            .expect("Failed to read final EOF packet");
        assert_eq!(final_eof[0], 0xfe, "Should get EOF packet at end");
    });

    // Kill server
    server.kill().expect("Failed to kill server");

    // Re-panic if test failed
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

fn compute_auth_response(password: &str, auth_data: &[u8]) -> Vec<u8> {
    use sha1::{Digest, Sha1};

    if password.is_empty() {
        return Vec::new();
    }

    // SHA1(password)
    let mut hasher = Sha1::new();
    hasher.update(password.as_bytes());
    let stage1 = hasher.finalize();

    // SHA1(SHA1(password))
    let mut hasher = Sha1::new();
    hasher.update(&stage1);
    let stage2 = hasher.finalize();

    // SHA1(auth_data + SHA1(SHA1(password)))
    let mut hasher = Sha1::new();
    hasher.update(auth_data);
    hasher.update(&stage2);
    let result = hasher.finalize();

    // XOR with SHA1(password)
    stage1
        .iter()
        .zip(result.iter())
        .map(|(a, b)| a ^ b)
        .collect()
}
