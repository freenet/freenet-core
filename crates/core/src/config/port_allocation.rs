use once_cell::sync::Lazy;
use rand::Rng;
use std::collections::HashMap;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

// Track recently used ports to avoid reuse conflicts
static RECENT_PORTS: Lazy<Mutex<HashMap<u16, SystemTime>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
// Track the last allocated port to try sequential allocation first
static LAST_PORT: AtomicU16 = AtomicU16::new(49152);

pub(crate) fn find_available_port() -> std::io::Result<u16> {
    const PORT_REUSE_TIMEOUT: Duration = Duration::from_secs(120); // 2 minutes
    const MAX_ATTEMPTS: u32 = 100;

    let now = SystemTime::now();
    let mut recent_ports = RECENT_PORTS.lock().unwrap();

    // Clean up expired entries
    recent_ports.retain(|_, &mut time| {
        now.duration_since(time)
            .map(|age| age < PORT_REUSE_TIMEOUT)
            .unwrap_or(false)
    });

    // First try sequential allocation from last successful port
    let mut attempts = 0;
    while attempts < MAX_ATTEMPTS {
        attempts += 1;

        let port = LAST_PORT.fetch_add(1, Ordering::Relaxed);
        if port >= 49152 {
            LAST_PORT.store(49152, Ordering::Relaxed);
            break; // Switch to random allocation
        }

        if recent_ports.contains_key(&port) {
            continue;
        }

        // Try to create socket with SO_REUSEADDR
        if let Ok(_socket) = UdpSocket::bind(("127.0.0.1", port)) {
            recent_ports.insert(port, now);
            return Ok(port);
        }
    }

    // Fall back to random allocation
    attempts = 0;
    while attempts < MAX_ATTEMPTS {
        attempts += 1;
        let port = rand::thread_rng().gen_range(49152..=65535);

        if recent_ports.contains_key(&port) {
            continue;
        }

        if let Ok(_socket) = UdpSocket::bind(("127.0.0.1", port)) {
            recent_ports.insert(port, now);
            LAST_PORT.store(port, Ordering::Relaxed);
            return Ok(port);
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::AddrInUse,
        "Could not find an available port after multiple attempts",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_port_is_immediately_available() -> std::io::Result<()> {
        // Get an available port
        let port = find_available_port()?;

        // Verify we can immediately bind to it
        let _socket = UdpSocket::bind(("127.0.0.1", port))?;

        // Verify we can also connect to it
        let client = UdpSocket::bind("127.0.0.1:0")?;
        client.connect(("127.0.0.1", port))?;

        Ok(())
    }

    #[test]
    fn test_multiple_port_allocations() -> std::io::Result<()> {
        // Get multiple ports in succession
        let port1 = find_available_port()?;
        let port2 = find_available_port()?;
        let port3 = find_available_port()?;

        // Verify they're all different
        assert_ne!(port1, port2);
        assert_ne!(port2, port3);
        assert_ne!(port1, port3);

        // Verify they're all in the expected range
        assert!(port1 >= 49152);
        assert!(port2 >= 49152);
        assert!(port3 >= 49152);

        Ok(())
    }
}
