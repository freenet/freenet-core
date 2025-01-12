use std::net::UdpSocket;

pub(crate) fn find_available_port() -> std::io::Result<u16> {
    // Bind to port 0 to let OS assign an available port
    let socket = UdpSocket::bind("127.0.0.1:0")?;
    socket.local_addr().map(|addr| addr.port())
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

        // Verify ports are in valid range (above 1024 for unprivileged ports)
        assert!(port1 > 1024);
        assert!(port2 > 1024);
        assert!(port3 > 1024);

        Ok(())
    }
}
