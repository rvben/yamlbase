use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

#[test]
fn test_mysql_auth_success() {
    let mut server = start_test_server();

    std::thread::sleep(Duration::from_secs(2));

    let result = std::panic::catch_unwind(|| {
        let mut stream =
            TcpStream::connect("127.0.0.1:13307").expect("Failed to connect to server");

        let (auth_data, sequence) = read_handshake(&mut stream);

        // Send correct credentials
        send_auth_response(
            &mut stream,
            sequence,
            "dbadmin",
            "securepass123",
            &auth_data,
        );

        let auth_result = read_auth_result(&mut stream);
        assert_eq!(
            auth_result, 0x00,
            "Authentication should succeed with correct credentials"
        );
    });

    server.kill().expect("Failed to kill server");

    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

#[test]
fn test_mysql_auth_wrong_username() {
    let mut server = start_test_server();

    std::thread::sleep(Duration::from_secs(2));

    let result = std::panic::catch_unwind(|| {
        let mut stream =
            TcpStream::connect("127.0.0.1:13307").expect("Failed to connect to server");

        let (auth_data, sequence) = read_handshake(&mut stream);

        // Send wrong username
        send_auth_response(
            &mut stream,
            sequence,
            "wronguser",
            "securepass123",
            &auth_data,
        );

        let auth_result = read_auth_result(&mut stream);
        assert_eq!(
            auth_result, 0xff,
            "Authentication should fail with wrong username"
        );
    });

    server.kill().expect("Failed to kill server");

    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

#[test]
fn test_mysql_auth_wrong_password() {
    let mut server = start_test_server();

    std::thread::sleep(Duration::from_secs(2));

    let result = std::panic::catch_unwind(|| {
        let mut stream =
            TcpStream::connect("127.0.0.1:13307").expect("Failed to connect to server");

        let (auth_data, sequence) = read_handshake(&mut stream);

        // Send wrong password
        send_auth_response(&mut stream, sequence, "dbadmin", "wrongpass", &auth_data);

        let auth_result = read_auth_result(&mut stream);
        assert_eq!(
            auth_result, 0xff,
            "Authentication should fail with wrong password"
        );
    });

    server.kill().expect("Failed to kill server");

    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

#[test]
fn test_mysql_auth_empty_password() {
    let mut server = start_test_server();

    std::thread::sleep(Duration::from_secs(2));

    let result = std::panic::catch_unwind(|| {
        let mut stream =
            TcpStream::connect("127.0.0.1:13307").expect("Failed to connect to server");

        let (auth_data, sequence) = read_handshake(&mut stream);

        // Send empty password
        send_auth_response(&mut stream, sequence, "dbadmin", "", &auth_data);

        let auth_result = read_auth_result(&mut stream);
        assert_eq!(
            auth_result, 0xff,
            "Authentication should fail with empty password"
        );
    });

    server.kill().expect("Failed to kill server");

    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

// Helper functions

fn start_test_server() -> std::process::Child {
    std::process::Command::new("cargo")
        .args(&[
            "run",
            "--",
            "-f",
            "examples/database_with_auth.yaml",
            "--protocol",
            "mysql",
            "-p",
            "13307",
        ])
        .spawn()
        .expect("Failed to start server")
}

fn read_handshake(stream: &mut TcpStream) -> (Vec<u8>, u8) {
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
    let auth_data_2 =
        &handshake[pos + 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10..pos + 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10 + 12];
    let mut auth_data = auth_data_1.to_vec();
    auth_data.extend_from_slice(auth_data_2);

    (auth_data, sequence)
}

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
    response.push(0); // null terminator

    // Compute auth response
    let auth_response = compute_auth_response(password, auth_data);
    response.push(auth_response.len() as u8);
    response.extend(&auth_response);

    // Write packet
    let mut packet = Vec::new();
    packet.extend(&(response.len() as u32).to_le_bytes()[..3]);
    packet.push(sequence + 1);
    packet.extend(&response);
    stream
        .write_all(&packet)
        .expect("Failed to send handshake response");
}

fn read_auth_result(stream: &mut TcpStream) -> u8 {
    let mut header = [0u8; 4];
    stream
        .read_exact(&mut header)
        .expect("Failed to read auth response header");
    let length = u32::from_le_bytes([header[0], header[1], header[2], 0]);
    let mut auth_result = vec![0u8; length as usize];
    stream
        .read_exact(&mut auth_result)
        .expect("Failed to read auth response");

    auth_result[0]
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

    stage1
        .iter()
        .zip(result.iter())
        .map(|(a, b)| a ^ b)
        .collect()
}
