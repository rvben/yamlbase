use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::NamedTempFile;

struct TestServer {
    port: u16,
    process: Child,
}

impl TestServer {
    fn start() -> Self {
        // Create temporary YAML file
        let mut temp_file = NamedTempFile::new().unwrap();
        let yaml_content = r#"
database:
  name: "test_db"
  auth:
    username: "root"
    password: "password"

tables:
  users:
    columns:
      id: "INTEGER PRIMARY KEY"
      name: "TEXT"
    data:
      - id: 1
        name: "Test User"
"#;
        temp_file.write_all(yaml_content.as_bytes()).unwrap();
        temp_file.flush().unwrap();

        // Get a free port
        let port = get_free_port();

        // Start server
        let process = Command::new("cargo")
            .args(&[
                "run",
                "--",
                "-f",
                temp_file.path().to_str().unwrap(),
                "--protocol",
                "mysql",
                "-p",
                &port.to_string(),
                "--log-level",
                "debug",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("Failed to start server");

        // Wait for server to be ready
        wait_for_port(port, Duration::from_secs(10));

        // Keep temp file alive
        std::mem::forget(temp_file);

        Self { port, process }
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

fn wait_for_port(port: u16, timeout: Duration) {
    let start = std::time::Instant::now();
    loop {
        if TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
            return;
        }
        if start.elapsed() >= timeout {
            panic!("Server didn't start within timeout");
        }
        thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn test_mysql_caching_sha2_authentication() {
    let server = TestServer::start();

    // Connect to MySQL server
    let mut stream =
        TcpStream::connect(format!("127.0.0.1:{}", server.port)).expect("Failed to connect");

    // Read initial handshake
    let mut buf = vec![0u8; 1024];
    let n = stream.read(&mut buf).unwrap();
    assert!(n > 0);

    // Extract auth data from handshake
    let auth_data = extract_auth_data(&buf[..n]);

    // Build handshake response with caching_sha2_password plugin
    let mut response = Vec::new();

    // Client capabilities
    response.extend(&0x8d_a6_2f_00u32.to_le_bytes()); // Include CLIENT_PLUGIN_AUTH

    // Max packet size
    response.extend(&0x00_00_00_01u32.to_le_bytes());

    // Character set (utf8mb4)
    response.push(33);

    // Reserved
    response.extend(&[0; 23]);

    // Username
    response.extend(b"root\0");

    // Auth response length (empty for initial response)
    response.push(0);

    // Database (none)

    // Auth plugin name
    response.extend(b"caching_sha2_password\0");

    // Send handshake response
    write_packet(&mut stream, 1, &response);

    // Read response - should be auth switch request
    let packet = read_packet(&mut stream);
    assert!(!packet.is_empty());

    if packet[0] == 0xfe {
        // Auth switch request
        println!("Received auth switch request");

        // Extract new auth data
        let plugin_data_start = packet[1..].iter().position(|&b| b == 0).unwrap() + 2;
        let new_auth_data = &packet[plugin_data_start..packet.len() - 1];

        // Compute auth response for caching_sha2_password with wrong password to test full auth
        let auth_response = compute_caching_sha2_response("wrongpassword", new_auth_data);

        // Send auth response
        write_packet(&mut stream, 3, &auth_response);

        // Read next packet
        let packet = read_packet(&mut stream);

        if packet.len() >= 2 && packet[0] == 0x01 && packet[1] == 0x04 {
            // Server requests full authentication
            println!("Server requested full authentication");

            // Send password in clear text (null-terminated)
            let mut clear_password = b"password".to_vec();
            clear_password.push(0);
            write_packet(&mut stream, 5, &clear_password);

            // Read final response
            let packet = read_packet(&mut stream);
            assert_eq!(packet[0], 0x00); // OK packet
            println!("Authentication successful!");
        } else if packet[0] == 0x00 {
            // Direct OK packet
            println!("Fast authentication successful!");
        } else {
            panic!("Unexpected response: {:?}", packet);
        }
    } else if packet[0] == 0x00 {
        // Direct OK packet (shouldn't happen with caching_sha2_password request)
        panic!("Server didn't switch to caching_sha2_password");
    } else {
        panic!("Authentication failed: {:?}", packet);
    }

    // Send a simple query to verify connection
    let query = b"SELECT 1";
    let mut query_packet = vec![0x03]; // COM_QUERY
    query_packet.extend_from_slice(query);
    write_packet(&mut stream, 0, &query_packet);

    // Read query response
    let packet = read_packet(&mut stream);
    assert!(packet[0] == 1 || packet[0] == 0xfb); // Column count or local infile
}

fn extract_auth_data(handshake: &[u8]) -> Vec<u8> {
    // Skip header and find auth data
    let mut pos = 4; // Skip packet header
    pos += 1; // Protocol version

    // Skip server version (null-terminated)
    while pos < handshake.len() && handshake[pos] != 0 {
        pos += 1;
    }
    pos += 1;

    pos += 4; // Connection ID

    // Auth data part 1 (8 bytes)
    let auth_part1 = &handshake[pos..pos + 8];
    pos += 8;

    pos += 1; // Filler
    pos += 2; // Capability flags (lower)
    pos += 1; // Character set
    pos += 2; // Status flags
    pos += 2; // Capability flags (upper)
    pos += 1; // Auth data length
    pos += 10; // Reserved

    // Auth data part 2 (12 bytes)
    let auth_part2 = &handshake[pos..pos + 12];

    let mut auth_data = Vec::new();
    auth_data.extend_from_slice(auth_part1);
    auth_data.extend_from_slice(auth_part2);
    auth_data
}

fn compute_caching_sha2_response(password: &str, auth_data: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};

    if password.is_empty() {
        return Vec::new();
    }

    // SHA256(password)
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    let stage1 = hasher.finalize();

    // SHA256(SHA256(password))
    let mut hasher = Sha256::new();
    hasher.update(&stage1);
    let stage2 = hasher.finalize();

    // SHA256(SHA256(SHA256(password)) + auth_data)
    let mut hasher = Sha256::new();
    hasher.update(&stage2);
    hasher.update(auth_data);
    let stage3 = hasher.finalize();

    // XOR with SHA256(password)
    stage1
        .iter()
        .zip(stage3.iter())
        .map(|(a, b)| a ^ b)
        .collect()
}

fn write_packet(stream: &mut TcpStream, seq_id: u8, data: &[u8]) {
    let mut packet = Vec::new();

    // Length (3 bytes, little-endian)
    packet.push((data.len() & 0xff) as u8);
    packet.push(((data.len() >> 8) & 0xff) as u8);
    packet.push(((data.len() >> 16) & 0xff) as u8);

    // Sequence ID
    packet.push(seq_id);

    // Payload
    packet.extend_from_slice(data);

    stream.write_all(&packet).unwrap();
    stream.flush().unwrap();
}

fn read_packet(stream: &mut TcpStream) -> Vec<u8> {
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).unwrap();

    let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);

    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).unwrap();

    payload
}
