//! Linux-specific sendmmsg implementation for batch UDP sends.
//!
//! Uses the `sendmmsg` syscall (Linux 3.0+) to send multiple UDP packets
//! in a single syscall, reducing syscall overhead by up to 100x.

use std::io;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::raw::c_int;

use super::BATCH_SIZE;

/// Send multiple UDP packets in a single syscall using sendmmsg.
///
/// # Arguments
/// * `fd` - The raw file descriptor of the UDP socket
/// * `packets` - Slice of (data, destination) pairs to send
///
/// # Returns
/// * `Ok(n)` - Number of packets successfully sent
/// * `Err(e)` - I/O error if the syscall failed
pub fn send_batch(fd: c_int, packets: &[(&[u8], SocketAddr)]) -> io::Result<usize> {
    if packets.is_empty() {
        return Ok(0);
    }

    let count = packets.len().min(BATCH_SIZE);

    // Allocate arrays on stack for syscall structures
    let mut hdrs: [libc::mmsghdr; BATCH_SIZE] = unsafe { std::mem::zeroed() };
    let mut iovecs: [libc::iovec; BATCH_SIZE] = unsafe { std::mem::zeroed() };
    let mut addrs: [SockAddrStorage; BATCH_SIZE] = [SockAddrStorage::new(); BATCH_SIZE];

    // Prepare message headers
    for (i, (buf, addr)) in packets.iter().take(count).enumerate() {
        // Convert SocketAddr to raw sockaddr
        let (sockaddr, socklen) = addrs[i].set(*addr);

        // Set up iovec (scatter-gather I/O)
        iovecs[i] = libc::iovec {
            iov_base: buf.as_ptr() as *mut libc::c_void,
            iov_len: buf.len(),
        };

        // Set up msghdr
        hdrs[i].msg_hdr = libc::msghdr {
            msg_name: sockaddr as *mut libc::c_void,
            msg_namelen: socklen,
            msg_iov: &mut iovecs[i],
            msg_iovlen: 1,
            msg_control: std::ptr::null_mut(),
            msg_controllen: 0,
            msg_flags: 0,
        };
    }

    // Call sendmmsg
    let ret = unsafe { libc::sendmmsg(fd, hdrs.as_mut_ptr(), count as libc::c_uint, 0) };

    if ret < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(ret as usize)
    }
}

/// Helper struct to hold sockaddr storage for both IPv4 and IPv6.
/// Must be kept alive for the duration of the sendmmsg call.
#[derive(Clone, Copy)]
struct SockAddrStorage {
    v4: libc::sockaddr_in,
    v6: libc::sockaddr_in6,
    is_v6: bool,
}

impl SockAddrStorage {
    const fn new() -> Self {
        Self {
            v4: unsafe { std::mem::zeroed() },
            v6: unsafe { std::mem::zeroed() },
            is_v6: false,
        }
    }

    /// Set the address and return a pointer to the sockaddr and its length.
    fn set(&mut self, addr: SocketAddr) -> (*mut libc::sockaddr, libc::socklen_t) {
        match addr {
            SocketAddr::V4(v4) => {
                self.is_v6 = false;
                self.v4 = sockaddr_v4(&v4);
                (
                    &mut self.v4 as *mut _ as *mut libc::sockaddr,
                    std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
                )
            }
            SocketAddr::V6(v6) => {
                self.is_v6 = true;
                self.v6 = sockaddr_v6(&v6);
                (
                    &mut self.v6 as *mut _ as *mut libc::sockaddr,
                    std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
                )
            }
        }
    }
}

/// Convert SocketAddrV4 to libc::sockaddr_in
fn sockaddr_v4(addr: &SocketAddrV4) -> libc::sockaddr_in {
    libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: addr.port().to_be(),
        sin_addr: libc::in_addr {
            s_addr: u32::from_ne_bytes(addr.ip().octets()),
        },
        sin_zero: [0; 8],
    }
}

/// Convert SocketAddrV6 to libc::sockaddr_in6
fn sockaddr_v6(addr: &SocketAddrV6) -> libc::sockaddr_in6 {
    libc::sockaddr_in6 {
        sin6_family: libc::AF_INET6 as libc::sa_family_t,
        sin6_port: addr.port().to_be(),
        sin6_flowinfo: addr.flowinfo(),
        sin6_addr: libc::in6_addr {
            s6_addr: addr.ip().octets(),
        },
        sin6_scope_id: addr.scope_id(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::UdpSocket;
    use std::os::unix::io::AsRawFd;

    #[test]
    fn test_send_batch_empty() {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let result = send_batch(socket.as_raw_fd(), &[]);
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_send_batch_single_packet() {
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        let receiver_addr = receiver.local_addr().unwrap();

        let data = b"hello";
        let packets: Vec<(&[u8], SocketAddr)> = vec![(data.as_slice(), receiver_addr)];

        let sent = send_batch(sender.as_raw_fd(), &packets).unwrap();
        assert_eq!(sent, 1);

        let mut buf = [0u8; 100];
        receiver
            .set_read_timeout(Some(std::time::Duration::from_millis(100)))
            .unwrap();
        let (len, _) = receiver.recv_from(&mut buf).unwrap();
        assert_eq!(&buf[..len], b"hello");
    }

    #[test]
    fn test_send_batch_multiple_packets() {
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        let receiver_addr = receiver.local_addr().unwrap();

        let packets: Vec<(&[u8], SocketAddr)> = (0..10)
            .map(|i| {
                let data: &[u8] = match i {
                    0 => b"packet0",
                    1 => b"packet1",
                    2 => b"packet2",
                    3 => b"packet3",
                    4 => b"packet4",
                    5 => b"packet5",
                    6 => b"packet6",
                    7 => b"packet7",
                    8 => b"packet8",
                    _ => b"packet9",
                };
                (data, receiver_addr)
            })
            .collect();

        let sent = send_batch(sender.as_raw_fd(), &packets).unwrap();
        assert_eq!(sent, 10);

        // Receive all packets
        receiver
            .set_read_timeout(Some(std::time::Duration::from_millis(100)))
            .unwrap();
        let mut received = 0;
        let mut buf = [0u8; 100];
        while received < 10 {
            match receiver.recv_from(&mut buf) {
                Ok(_) => received += 1,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => panic!("recv error: {}", e),
            }
        }
        assert_eq!(received, 10);
    }
}
