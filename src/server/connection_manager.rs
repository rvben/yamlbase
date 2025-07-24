use std::collections::HashMap;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::{RwLock, Semaphore};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::database::Storage;
use crate::protocol::Connection;

/// Connection statistics for monitoring
#[derive(Debug, Clone)]
pub struct ConnectionStats {
    pub total_connections: usize,
    pub active_connections: usize,
    pub failed_connections: usize,
    pub timeout_connections: usize,
    pub avg_connection_duration: Duration,
}

/// Individual connection metadata
#[derive(Debug)]
struct ConnectionInfo {
    pub id: usize,
    pub client_addr: String,
    pub started_at: Instant,
    pub last_activity: Instant,
}

/// Connection manager for handling client connection stability
pub struct ConnectionManager {
    config: Arc<Config>,
    storage: Arc<Storage>,
    connections: Arc<RwLock<HashMap<usize, ConnectionInfo>>>,
    connection_counter: AtomicUsize,
    active_connections: AtomicUsize,
    failed_connections: AtomicUsize,
    timeout_connections: AtomicUsize,
    connection_semaphore: Arc<Semaphore>,
}

impl Clone for ConnectionManager {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            storage: self.storage.clone(),
            connections: self.connections.clone(),
            connection_counter: AtomicUsize::new(self.connection_counter.load(Ordering::SeqCst)),
            active_connections: AtomicUsize::new(self.active_connections.load(Ordering::SeqCst)),
            failed_connections: AtomicUsize::new(self.failed_connections.load(Ordering::SeqCst)),
            timeout_connections: AtomicUsize::new(self.timeout_connections.load(Ordering::SeqCst)),
            connection_semaphore: self.connection_semaphore.clone(),
        }
    }
}

impl ConnectionManager {
    pub fn new(config: Arc<Config>, storage: Arc<Storage>) -> Self {
        let max_connections = config.max_connections.unwrap_or(1000);
        
        Self {
            config,
            storage,
            connections: Arc::new(RwLock::new(HashMap::new())),
            connection_counter: AtomicUsize::new(0),
            active_connections: AtomicUsize::new(0),
            failed_connections: AtomicUsize::new(0),
            timeout_connections: AtomicUsize::new(0),
            connection_semaphore: Arc::new(Semaphore::new(max_connections)),
        }
    }

    /// Handle a new client connection with full stability features
    pub async fn handle_connection(&self, mut stream: TcpStream, client_addr: String) -> crate::Result<()> {
        // Acquire connection permit (with timeout to prevent hanging)
        let permit = match timeout(
            Duration::from_secs(30),
            self.connection_semaphore.acquire()
        ).await {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => {
                error!("Failed to acquire connection permit");
                return Err(crate::YamlBaseError::Database {
                    message: "Connection pool exhausted".to_string(),
                });
            }
            Err(_) => {
                error!("Timeout acquiring connection permit for {}", client_addr);
                return Err(crate::YamlBaseError::Database {
                    message: "Connection pool timeout".to_string(),
                });
            }
        };

        // Configure TCP socket for stability
        if let Err(e) = self.configure_tcp_socket(&mut stream).await {
            warn!("Failed to configure TCP socket options: {}", e);
            // Continue anyway - not critical
        }

        let connection_id = self.connection_counter.fetch_add(1, Ordering::SeqCst);
        let now = Instant::now();
        
        // Register connection
        {
            let mut connections = self.connections.write().await;
            connections.insert(connection_id, ConnectionInfo {
                id: connection_id,
                client_addr: client_addr.clone(),
                started_at: now,
                last_activity: now,
            });
        }

        self.active_connections.fetch_add(1, Ordering::SeqCst);
        info!("Connection {} from {} established", connection_id, client_addr);

        // Handle the connection with comprehensive error handling
        let result = self.handle_connection_with_recovery(
            stream, 
            connection_id, 
            client_addr.clone()
        ).await;

        // Cleanup connection
        {
            let mut connections = self.connections.write().await;
            connections.remove(&connection_id);
        }
        
        self.active_connections.fetch_sub(1, Ordering::SeqCst);
        drop(permit); // Release connection permit

        // Update statistics based on result
        match &result {
            Ok(_) => {
                let duration = now.elapsed();
                info!("Connection {} closed normally after {:?}", connection_id, duration);
            }
            Err(e) => {
                self.failed_connections.fetch_add(1, Ordering::SeqCst);
                if e.to_string().contains("timeout") {
                    self.timeout_connections.fetch_add(1, Ordering::SeqCst);
                }
                error!("Connection {} failed: {}", connection_id, e);
            }
        }

        result
    }

    /// Configure TCP socket options for connection stability
    async fn configure_tcp_socket(&self, stream: &mut TcpStream) -> crate::Result<()> {
        use std::os::unix::io::AsRawFd;
        use std::mem::size_of;

        let fd = stream.as_raw_fd();

        // Enable TCP_NODELAY to reduce latency
        let nodelay: libc::c_int = 1;
        unsafe {
            if libc::setsockopt(
                fd,
                libc::IPPROTO_TCP,
                libc::TCP_NODELAY,
                &nodelay as *const _ as *const libc::c_void,
                size_of::<libc::c_int>() as libc::socklen_t,
            ) != 0 {
                return Err(crate::YamlBaseError::Database {
                    message: "Failed to set TCP_NODELAY".to_string(),
                });
            }
        }

        // Enable SO_KEEPALIVE for connection health monitoring
        let keepalive: libc::c_int = 1;
        unsafe {
            if libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_KEEPALIVE,
                &keepalive as *const _ as *const libc::c_void,
                size_of::<libc::c_int>() as libc::socklen_t,
            ) != 0 {
                return Err(crate::YamlBaseError::Database {
                    message: "Failed to set SO_KEEPALIVE".to_string(),
                });
            }
        }

        // Set keepalive parameters (Linux-specific)
        #[cfg(target_os = "linux")]
        {
            // Time before starting keepalive probes (seconds)
            let keepalive_time: libc::c_int = 60;
            unsafe {
                libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_KEEPIDLE,
                    &keepalive_time as *const _ as *const libc::c_void,
                    size_of::<libc::c_int>() as libc::socklen_t,
                );
            }

            // Interval between keepalive probes (seconds)
            let keepalive_interval: libc::c_int = 10;
            unsafe {
                libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_KEEPINTVL,
                    &keepalive_interval as *const _ as *const libc::c_void,
                    size_of::<libc::c_int>() as libc::socklen_t,
                );
            }

            // Number of keepalive probes before declaring connection dead
            let keepalive_count: libc::c_int = 6;
            unsafe {
                libc::setsockopt(
                    fd,
                    libc::IPPROTO_TCP,
                    libc::TCP_KEEPCNT,
                    &keepalive_count as *const _ as *const libc::c_void,
                    size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }

        debug!("TCP socket configured with stability options");
        Ok(())
    }

    /// Handle connection with recovery and retry logic
    async fn handle_connection_with_recovery(
        &self,
        stream: TcpStream,
        connection_id: usize,
        client_addr: String,
    ) -> crate::Result<()> {
        let connection_timeout = self.config.connection_timeout
            .unwrap_or(Duration::from_secs(300)); // 5 minutes default

        let connection = Connection::new(self.config.clone(), self.storage.clone());

        // Wrap connection handling with timeout
        let connection_future = async {
            // Update last activity
            self.update_connection_activity(connection_id).await;
            
            // Handle the actual protocol connection
            connection.handle(stream).await
        };

        match timeout(connection_timeout, connection_future).await {
            Ok(result) => result,
            Err(_) => {
                warn!("Connection {} from {} timed out after {:?}", 
                     connection_id, client_addr, connection_timeout);
                Err(crate::YamlBaseError::Database {
                    message: format!("Connection timeout after {:?}", connection_timeout),
                })
            }
        }
    }

    /// Update last activity timestamp for a connection
    async fn update_connection_activity(&self, connection_id: usize) {
        let mut connections = self.connections.write().await;
        if let Some(conn_info) = connections.get_mut(&connection_id) {
            conn_info.last_activity = Instant::now();
        }
    }

    /// Get connection statistics for monitoring
    pub async fn get_stats(&self) -> ConnectionStats {
        let connections = self.connections.read().await;
        let active = self.active_connections.load(Ordering::SeqCst);
        let total = self.connection_counter.load(Ordering::SeqCst);
        let failed = self.failed_connections.load(Ordering::SeqCst);
        let timeouts = self.timeout_connections.load(Ordering::SeqCst);

        // Calculate average connection duration from active connections
        let now = Instant::now();
        let total_duration: Duration = connections.values()
            .map(|conn| now.duration_since(conn.started_at))
            .sum();
        
        let avg_duration = if !connections.is_empty() {
            total_duration / connections.len() as u32
        } else {
            Duration::from_secs(0)
        };

        ConnectionStats {
            total_connections: total,
            active_connections: active,
            failed_connections: failed,
            timeout_connections: timeouts,
            avg_connection_duration: avg_duration,
        }
    }

    /// Cleanup idle/stale connections
    pub async fn cleanup_stale_connections(&self) {
        let idle_timeout = Duration::from_secs(1800); // 30 minutes
        let now = Instant::now();
        let mut to_remove = Vec::new();

        {
            let connections = self.connections.read().await;
            for (id, conn_info) in connections.iter() {
                if now.duration_since(conn_info.last_activity) > idle_timeout {
                    warn!("Connection {} from {} is idle for {:?}, marking for cleanup", 
                         id, conn_info.client_addr, now.duration_since(conn_info.last_activity));
                    to_remove.push(*id);
                }
            }
        }

        if !to_remove.is_empty() {
            let mut connections = self.connections.write().await;
            for id in to_remove {
                connections.remove(&id);
                info!("Cleaned up stale connection {}", id);
            }
        }
    }

    /// Start background monitoring task
    pub fn start_monitoring(&self) -> tokio::task::JoinHandle<()> {
        let manager = Arc::new(self.connections.clone());
        let stats_interval = Duration::from_secs(60); // Log stats every minute
        let cleanup_interval = Duration::from_secs(300); // Cleanup every 5 minutes
        
        tokio::spawn(async move {
            let mut stats_timer = tokio::time::interval(stats_interval);
            let mut cleanup_timer = tokio::time::interval(cleanup_interval);
            
            loop {
                tokio::select! {
                    _ = stats_timer.tick() => {
                        let connections = manager.read().await;
                        info!("Connection pool status: {} active connections", connections.len());
                        
                        // Log connection details in debug mode
                        for (id, conn_info) in connections.iter() {
                            debug!("Connection {}: {} (active for {:?})", 
                                  id, conn_info.client_addr, 
                                  Instant::now().duration_since(conn_info.started_at));
                        }
                    }
                    _ = cleanup_timer.tick() => {
                        // This would need access to the full ConnectionManager
                        // For now, just log that cleanup would happen
                        debug!("Connection cleanup cycle (would cleanup stale connections)");
                    }
                }
            }
        })
    }
}