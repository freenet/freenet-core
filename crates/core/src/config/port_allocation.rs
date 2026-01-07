use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
};

use super::GlobalRng;

const LOOPBACK_V4: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

/// High-numbered ports that sit safely outside of the Windows/WSL Hyper-V exclusion
/// bands reported in issue #2058. We randomize within these ranges before falling
/// back to OS-assigned ephemeral ports so other platforms retain the previous behavior.
const SAFE_WINDOWS_PORT_RANGES: &[(u16, u16)] = &[
    (31337, 31337), // keep the traditional port as an option if available
    (55000, 55999),
    (56000, 56999),
    (58000, 58999),
    (60000, 60999),
    (62000, 62999),
];

const MAX_SAFE_PORT_ATTEMPTS: usize = 256;

pub(crate) fn find_available_port() -> io::Result<u16> {
    if should_avoid_windows_excluded_ports() {
        if let Some(port) = pick_port_from_safe_windows_ranges() {
            return Ok(port);
        }
    }

    // Fallback: bind to port 0 to let the OS choose an available port.
    let socket = UdpSocket::bind(SocketAddr::new(LOOPBACK_V4, 0))?;
    socket.local_addr().map(|addr| addr.port())
}

fn pick_port_from_safe_windows_ranges() -> Option<u16> {
    let mut candidates: Vec<u16> = SAFE_WINDOWS_PORT_RANGES
        .iter()
        .flat_map(|(start, end)| *start..=*end)
        .collect();

    if candidates.is_empty() {
        return None;
    }

    GlobalRng::shuffle(&mut candidates);
    candidates
        .into_iter()
        .take(MAX_SAFE_PORT_ATTEMPTS)
        .find(|port| port_is_free(*port))
}

fn port_is_free(port: u16) -> bool {
    UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port)).is_ok()
}

fn should_avoid_windows_excluded_ports() -> bool {
    cfg!(windows) || is_wsl()
}

#[cfg(unix)]
fn is_wsl() -> bool {
    use std::{env, fs};

    if env::var_os("WSL_INTEROP").is_some() || env::var_os("WSL_DISTRO_NAME").is_some() {
        return true;
    }

    if let Ok(release) = fs::read_to_string("/proc/sys/kernel/osrelease") {
        let release = release.to_ascii_lowercase();
        if release.contains("microsoft") || release.contains("wsl") {
            return true;
        }
    }

    false
}

#[cfg(not(unix))]
fn is_wsl() -> bool {
    false
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

    #[test]
    fn test_safe_range_selection_is_within_expected_bounds() {
        if let Some(port) = pick_port_from_safe_windows_ranges() {
            assert!(
                SAFE_WINDOWS_PORT_RANGES
                    .iter()
                    .any(|(start, end)| (port >= *start) && (port <= *end)),
                "selected port {port} should fall within a safe range"
            );
        }
    }
}
