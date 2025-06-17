use bytes::{BufMut, BytesMut};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;

use crate::YamlBaseError;

// MySQL packet types
const AUTH_MORE_DATA: u8 = 0x01;
const AUTH_SWITCH_REQUEST: u8 = 0xfe;

// Caching SHA2 authentication states
const FAST_AUTH_SUCCESS: u8 = 0x03;
const PERFORM_FULL_AUTH: u8 = 0x04;

pub const CACHING_SHA2_PLUGIN_NAME: &str = "caching_sha2_password";

#[derive(Debug)]
pub struct CachingSha2Auth {
    auth_data: Vec<u8>,
}

impl CachingSha2Auth {
    pub fn new(auth_data: Vec<u8>) -> Self {
        Self { auth_data }
    }

    /// Handle the full caching_sha2_password authentication flow
    pub async fn authenticate(
        &self,
        stream: &mut TcpStream,
        sequence_id: &mut u8,
        username: &str,
        password: &str,
        expected_username: &str,
        expected_password: &str,
        auth_response: Vec<u8>,
    ) -> crate::Result<bool> {
        debug!(
            "Starting caching_sha2_password authentication for user: {}",
            username
        );

        // Verify username
        if username != expected_username {
            debug!("Username mismatch: {} != {}", username, expected_username);
            return Ok(false);
        }

        // If client sent empty auth response, we need to request full authentication
        if auth_response.is_empty() && !password.is_empty() {
            debug!("Empty auth response, requesting full authentication");
            self.send_auth_more_data(stream, sequence_id, PERFORM_FULL_AUTH)
                .await?;

            // Read the password
            let password_packet = self.read_packet(stream, sequence_id).await?;
            if password_packet.is_empty() {
                debug!("Empty password packet received");
                return Ok(false);
            }

            // Remove null terminator if present
            let client_password = if password_packet.last() == Some(&0) {
                std::str::from_utf8(&password_packet[..password_packet.len() - 1])
                    .map_err(|_| YamlBaseError::Protocol("Invalid UTF-8 in password".to_string()))?
            } else {
                std::str::from_utf8(&password_packet)
                    .map_err(|_| YamlBaseError::Protocol("Invalid UTF-8 in password".to_string()))?
            };

            debug!("Received password in clear text");
            return Ok(client_password == expected_password);
        }

        // Compute expected auth response
        let expected = compute_auth_response(expected_password, &self.auth_data);

        // Check if auth response matches
        if auth_response == expected {
            debug!("Fast authentication successful");
            // Send fast auth success
            self.send_auth_more_data(stream, sequence_id, FAST_AUTH_SUCCESS)
                .await?;
            return Ok(true);
        }

        // Auth failed, request full authentication
        debug!("Fast authentication failed, requesting full authentication");
        self.send_auth_more_data(stream, sequence_id, PERFORM_FULL_AUTH)
            .await?;

        // Read the password in clear text
        let password_packet = self.read_packet(stream, sequence_id).await?;
        if password_packet.is_empty() {
            debug!("Empty password packet received");
            return Ok(false);
        }

        // Remove null terminator if present
        let client_password = if password_packet.last() == Some(&0) {
            std::str::from_utf8(&password_packet[..password_packet.len() - 1])
                .map_err(|_| YamlBaseError::Protocol("Invalid UTF-8 in password".to_string()))?
        } else {
            std::str::from_utf8(&password_packet)
                .map_err(|_| YamlBaseError::Protocol("Invalid UTF-8 in password".to_string()))?
        };

        debug!("Checking clear text password");
        Ok(client_password == expected_password)
    }

    /// Send an auth more data packet
    async fn send_auth_more_data(
        &self,
        stream: &mut TcpStream,
        sequence_id: &mut u8,
        status: u8,
    ) -> crate::Result<()> {
        let mut packet = BytesMut::new();
        packet.put_u8(AUTH_MORE_DATA);
        packet.put_u8(status);

        self.write_packet(stream, sequence_id, &packet).await
    }

    /// Send an auth switch request
    pub async fn send_auth_switch_request(
        &self,
        stream: &mut TcpStream,
        sequence_id: &mut u8,
    ) -> crate::Result<()> {
        debug!("Sending auth switch request for caching_sha2_password");

        let mut packet = BytesMut::new();
        packet.put_u8(AUTH_SWITCH_REQUEST);
        packet.put_slice(CACHING_SHA2_PLUGIN_NAME.as_bytes());
        packet.put_u8(0); // null terminator
        packet.put_slice(&self.auth_data);
        packet.put_u8(0); // null terminator

        self.write_packet(stream, sequence_id, &packet).await
    }

    async fn write_packet(
        &self,
        stream: &mut TcpStream,
        sequence_id: &mut u8,
        payload: &[u8],
    ) -> crate::Result<()> {
        let mut packet = BytesMut::with_capacity(4 + payload.len());

        // Length (3 bytes)
        packet.put_u8((payload.len() & 0xff) as u8);
        packet.put_u8(((payload.len() >> 8) & 0xff) as u8);
        packet.put_u8(((payload.len() >> 16) & 0xff) as u8);

        // Sequence ID
        packet.put_u8(*sequence_id);

        debug!(
            "Writing caching_sha2 packet: len={}, seq={}, type={:02x}",
            payload.len(),
            *sequence_id,
            payload.get(0).unwrap_or(&0)
        );

        *sequence_id = sequence_id.wrapping_add(1);

        // Payload
        packet.put_slice(payload);

        stream.write_all(&packet).await?;
        stream.flush().await?;

        Ok(())
    }

    async fn read_packet(
        &self,
        stream: &mut TcpStream,
        sequence_id: &mut u8,
    ) -> crate::Result<Vec<u8>> {
        let mut header = [0u8; 4];
        stream.read_exact(&mut header).await?;

        let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
        *sequence_id = header[3].wrapping_add(1);

        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await?;

        debug!(
            "Read caching_sha2 packet: len={}, seq={}, first_bytes={:?}",
            len,
            header[3],
            &payload[..std::cmp::min(20, payload.len())]
        );

        Ok(payload)
    }
}

/// Compute the caching_sha2_password auth response
pub fn compute_auth_response(password: &str, auth_data: &[u8]) -> Vec<u8> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_auth_response() {
        // Test with known values
        let auth_data = b"12345678901234567890";
        let password = "password";

        let response = compute_auth_response(password, auth_data);
        assert_eq!(response.len(), 32); // SHA256 produces 32 bytes

        // Empty password should return empty response
        let empty_response = compute_auth_response("", auth_data);
        assert!(empty_response.is_empty());
    }
}
