use std::net::TcpListener;
use std::sync::atomic::{AtomicU16, Ordering};

// Start from a high port to avoid conflicts with common services
static NEXT_PORT: AtomicU16 = AtomicU16::new(40000);

/// Get a free port for testing by binding to port 0 and letting the OS assign
pub fn get_free_port() -> u16 {
    // Try binding to 0 to get an OS-assigned port
    match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => {
            let port = listener.local_addr().unwrap().port();
            drop(listener); // Release the port immediately
            port
        }
        Err(_) => {
            // Fallback to incrementing port numbers if OS assignment fails
            NEXT_PORT.fetch_add(1, Ordering::SeqCst)
        }
    }
}

/// Get a free port for a specific protocol (for documentation purposes)
pub fn get_free_port_for(protocol: &str) -> u16 {
    let port = get_free_port();
    tracing::debug!("Allocated port {} for {} testing", port, protocol);
    port
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_unique_ports() {
        let mut ports = HashSet::new();
        for _ in 0..10 {
            let port = get_free_port();
            assert!(ports.insert(port), "Port {} was already allocated", port);
            assert!(port > 1024, "Port {} is in privileged range", port);
        }
    }

    #[test]
    fn test_ports_are_actually_free() {
        let port = get_free_port();
        // Should be able to bind to the port we got
        let listener = TcpListener::bind(format!("127.0.0.1:{}", port));
        assert!(listener.is_ok(), "Could not bind to supposedly free port {}", port);
    }
}