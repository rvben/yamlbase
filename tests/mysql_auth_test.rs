#![allow(clippy::uninlined_format_args)]

mod common;

use common::TestServer;
use std::io::{Read, Write};
use std::net::TcpStream;

#[test]
#[ignore = "Flaky test - needs investigation"]
fn test_mysql_auth_success() {
    let server = TestServer::start_mysql("examples/database_with_auth.yaml");

    let result = std::panic::catch_unwind(|| {
        let mut stream = server.connect();

        // Read handshake packet
        let mut header = [0u8; 4];
        stream
            .read_exact(&mut header)
            .expect("Failed to read header");
        let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
        let sequence = header[3];

        let mut handshake = vec![0u8; length as usize];
        stream
            .read_exact(&mut handshake)
            .expect("Failed to read handshake");

        // Verify it's a handshake packet
        let protocol_version = handshake[0];
        assert_eq!(protocol_version, 10, "Expected MySQL protocol version 10");

        // Extract auth data for password hashing
        let pos = handshake.iter().position(|&b| b == 0).unwrap() + 1 + 4;
        let auth_data_1 = &handshake[pos..pos + 8];
        let auth_data_2 = &handshake
            [pos + 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10..pos + 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10 + 12];
        let mut auth_data = auth_data_1.to_vec();
        auth_data.extend_from_slice(auth_data_2);

        // Send correct auth response
        send_auth_response(
            &mut stream,
            sequence,
            "dbadmin",
            "securepass123",
            &auth_data,
        );

        // Should receive OK packet
        verify_auth_success(&mut stream);
    });

    if let Err(e) = result {
        eprintln!("Test failed: {:?}", e);
        panic!("Test failed");
    }
}

#[test]
#[ignore = "Flaky test - needs investigation"]
fn test_mysql_auth_wrong_password() {
    let server = TestServer::start_mysql("examples/database_with_auth.yaml");

    let result = std::panic::catch_unwind(|| {
        let mut stream = server.connect();

        // Read handshake packet
        let mut header = [0u8; 4];
        stream
            .read_exact(&mut header)
            .expect("Failed to read header");
        let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
        let sequence = header[3];

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

        // Send wrong password
        send_auth_response(
            &mut stream,
            sequence,
            "dbadmin",
            "wrongpassword",
            &auth_data,
        );

        // Should receive error packet
        verify_auth_failure(&mut stream);
    });

    if let Err(e) = result {
        eprintln!("Test failed: {:?}", e);
        panic!("Test failed");
    }
}

#[test]
#[ignore = "Flaky test - needs investigation"]
fn test_mysql_auth_wrong_username() {
    let server = TestServer::start_mysql("examples/database_with_auth.yaml");

    let result = std::panic::catch_unwind(|| {
        let mut stream = server.connect();

        // Read handshake packet
        let mut header = [0u8; 4];
        stream
            .read_exact(&mut header)
            .expect("Failed to read header");
        let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
        let sequence = header[3];

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

        // Send wrong username
        send_auth_response(
            &mut stream,
            sequence,
            "wronguser",
            "password123",
            &auth_data,
        );

        // Should receive error packet
        verify_auth_failure(&mut stream);
    });

    if let Err(e) = result {
        eprintln!("Test failed: {:?}", e);
        panic!("Test failed");
    }
}

#[test]
#[ignore = "Flaky test - needs investigation"]
fn test_mysql_auth_empty_password_modern_client() {
    let server = TestServer::start_mysql("examples/database_with_auth.yaml");

    let result = std::panic::catch_unwind(|| {
        let mut stream = server.connect();

        // Read handshake packet
        let mut header = [0u8; 4];
        stream
            .read_exact(&mut header)
            .expect("Failed to read header");
        let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
        let sequence = header[3];

        let mut handshake = vec![0u8; length as usize];
        stream
            .read_exact(&mut handshake)
            .expect("Failed to read handshake");

        // Modern client sends empty auth response expecting auth switch
        send_empty_auth_response(&mut stream, sequence, "dbadmin");

        // Should receive auth switch request
        let mut header = [0u8; 4];
        stream
            .read_exact(&mut header)
            .expect("Failed to read response header");
        let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);

        let mut response = vec![0u8; length as usize];
        stream
            .read_exact(&mut response)
            .expect("Failed to read response");

        // Since we're sending an empty auth response with a valid username,
        // the server should either:
        // 1. Send an auth switch request (0xfe) if it supports it
        // 2. Send an error packet (0xff) if it doesn't recognize empty auth
        // Both are valid responses for this test scenario

        if response[0] == 0xfe {
            // Server sent auth switch request
            // Extract plugin name
            let plugin_end = response[1..]
                .iter()
                .position(|&b| b == 0)
                .unwrap_or(response.len() - 1);
            let plugin_name = std::str::from_utf8(&response[1..1 + plugin_end]).unwrap_or("");

            if plugin_name == "caching_sha2_password" {
                // Send empty auth response for caching_sha2_password
                let packet = vec![0, 0, 0, header[3] + 1];
                stream
                    .write_all(&packet)
                    .expect("Failed to send empty auth response");

                // Read auth more data packet
                let mut header = [0u8; 4];
                stream
                    .read_exact(&mut header)
                    .expect("Failed to read header");
                let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);

                let mut auth_more = vec![0u8; length as usize];
                stream
                    .read_exact(&mut auth_more)
                    .expect("Failed to read auth more data");

                if auth_more[0] == 0x01 && auth_more.get(1) == Some(&0x04) {
                    // Server requests full authentication
                    // Send empty password
                    let packet = vec![1, 0, 0, header[3] + 1, 0];
                    stream
                        .write_all(&packet)
                        .expect("Failed to send empty password");

                    // Now should receive error
                    verify_auth_failure(&mut stream);
                } else {
                    panic!("Unexpected auth more data: {:?}", auth_more);
                }
            } else {
                // Old mysql_native_password flow
                let packet = vec![0, 0, 0, header[3] + 1];
                stream
                    .write_all(&packet)
                    .expect("Failed to send empty auth");
                verify_auth_failure(&mut stream);
            }
        } else if response[0] == 0x01 {
            // Server sent auth more data packet (caching_sha2_password)
            // Check the status byte
            if response.len() > 1 && response[1] == 0x04 {
                // Server is requesting full authentication
                // Send empty password (just null terminator)
                let packet = vec![1, 0, 0, header[3] + 1, 0];
                stream
                    .write_all(&packet)
                    .expect("Failed to send empty password");

                // Should receive error since password is required
                verify_auth_failure(&mut stream);
            } else {
                panic!(
                    "Unexpected auth more data status: 0x{:02x}",
                    response.get(1).unwrap_or(&0)
                );
            }
        } else if response[0] == 0xff {
            // Server sent error directly - this is also valid
            let error_code = u16::from_le_bytes([response[1], response[2]]);
            assert_eq!(error_code, 1045, "Expected access denied error");
        } else {
            panic!("Unexpected response packet type: 0x{:02x}", response[0]);
        }
    });

    if let Err(e) = result {
        eprintln!("Test failed: {:?}", e);
        panic!("Test failed");
    }
}

// Helper functions

fn send_auth_response(
    stream: &mut TcpStream,
    sequence: u8,
    username: &str,
    password: &str,
    auth_data: &[u8],
) {
    let mut response = Vec::new();
    response.extend(&0x000fa685u32.to_le_bytes()); // capabilities
    response.extend(&16777216u32.to_le_bytes()); // max packet
    response.push(33); // charset
    response.extend(&[0u8; 23]); // reserved
    response.extend(username.as_bytes());
    response.push(0);

    // Calculate auth response
    let auth_response = mysql_native_password_auth(auth_data, password);
    response.push(auth_response.len() as u8);
    response.extend(&auth_response);

    // Send response
    let mut packet = Vec::new();
    packet.extend(&(response.len() as u32).to_le_bytes()[..3]);
    packet.push(sequence + 1);
    packet.extend(&response);

    stream.write_all(&packet).expect("Failed to send auth");
}

fn send_empty_auth_response(stream: &mut TcpStream, sequence: u8, username: &str) {
    let mut response = Vec::new();
    response.extend(&0x000fa685u32.to_le_bytes()); // capabilities
    response.extend(&16777216u32.to_le_bytes()); // max packet
    response.push(33); // charset
    response.extend(&[0u8; 23]); // reserved
    response.extend(username.as_bytes());
    response.push(0);
    response.push(0); // empty auth response

    // Send response
    let mut packet = Vec::new();
    packet.extend(&(response.len() as u32).to_le_bytes()[..3]);
    packet.push(sequence + 1);
    packet.extend(&response);

    stream.write_all(&packet).expect("Failed to send auth");
}

fn verify_auth_success(stream: &mut TcpStream) {
    let mut header = [0u8; 4];
    stream
        .read_exact(&mut header)
        .expect("Failed to read response header");
    let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);

    let mut response = vec![0u8; length as usize];
    stream
        .read_exact(&mut response)
        .expect("Failed to read response");

    // Check for auth switch request
    if response[0] == 0xfe {
        // Server is requesting auth switch, send empty auth response
        let packet = vec![0, 0, 0, header[3] + 1];
        stream
            .write_all(&packet)
            .expect("Failed to send empty auth");

        // Read final OK packet
        let mut header = [0u8; 4];
        stream
            .read_exact(&mut header)
            .expect("Failed to read OK header");
        let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);

        let mut ok_packet = vec![0u8; length as usize];
        stream
            .read_exact(&mut ok_packet)
            .expect("Failed to read OK packet");

        assert_eq!(ok_packet[0], 0x00, "Expected OK packet after auth switch");
    } else {
        assert_eq!(response[0], 0x00, "Expected OK packet");
    }
}

fn verify_auth_failure(stream: &mut TcpStream) {
    let mut header = [0u8; 4];
    stream
        .read_exact(&mut header)
        .expect("Failed to read response header");
    let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);

    let mut response = vec![0u8; length as usize];
    stream
        .read_exact(&mut response)
        .expect("Failed to read response");

    assert_eq!(response[0], 0xff, "Expected error packet");
    let error_code = u16::from_le_bytes([response[1], response[2]]);
    assert_eq!(error_code, 1045, "Expected access denied error");
}

fn mysql_native_password_auth(auth_data: &[u8], password: &str) -> Vec<u8> {
    use sha1::{Digest, Sha1};

    if password.is_empty() {
        return vec![];
    }

    let mut hasher = Sha1::new();
    hasher.update(password.as_bytes());
    let password_hash = hasher.finalize();

    let mut hasher = Sha1::new();
    hasher.update(password_hash);
    let password_double_hash = hasher.finalize();

    let mut hasher = Sha1::new();
    hasher.update(auth_data);
    hasher.update(password_double_hash);
    let result = hasher.finalize();

    password_hash
        .iter()
        .zip(result.iter())
        .map(|(a, b)| a ^ b)
        .collect()
}
