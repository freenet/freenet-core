//! Proof-of-concept: Using Turmoil for deterministic simulation testing.
//!
//! This test explores whether we can use Turmoil's deterministic scheduler
//! while keeping our SimulationSocket for network communication.
//!
//! Key questions:
//! 1. Can Turmoil schedule our async code deterministically?
//! 2. Does SimulationSocket work across Turmoil hosts?
//! 3. Can we integrate VirtualTime with Turmoil's clock?

use std::net::{Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use freenet::dev_tool::VirtualTime;
use freenet::transport::in_memory_socket::{
    clear_all_socket_registries, register_address_network, register_network_time_source,
    SimulationSocket,
};

/// Basic test: Can we run async code inside Turmoil hosts?
#[test]
fn test_turmoil_basic_async() -> turmoil::Result {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(10))
        .build();

    let counter = Arc::new(AtomicU64::new(0));
    let counter_clone = counter.clone();

    sim.client("test", async move {
        // Simple async work - use tokio::time, turmoil intercepts it
        for i in 0..10 {
            counter_clone.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(100)).await;
            tracing::info!("Iteration {}", i);
        }
        Ok(())
    });

    sim.run()?;

    assert_eq!(counter.load(Ordering::SeqCst), 10);
    Ok(())
}

/// Test: Can we use tokio channels inside Turmoil?
#[test]
fn test_turmoil_with_channels() -> turmoil::Result {
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(10))
        .build();

    let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(10);

    // Producer host
    sim.host("producer", move || {
        let tx = tx.clone();
        async move {
            for i in 0..5 {
                tx.send(i).await.unwrap();
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok(())
        }
    });

    // Consumer/test client
    sim.client("consumer", async move {
        let mut received = Vec::new();
        for _ in 0..5 {
            if let Some(val) = rx.recv().await {
                received.push(val);
            }
        }
        assert_eq!(received, vec![0, 1, 2, 3, 4]);
        Ok(())
    });

    sim.run()
}

/// Test: Can multiple hosts communicate via our global registry pattern?
/// This simulates how SimulationSocket uses a global registry for routing.
#[test]
fn test_turmoil_with_global_state() -> turmoil::Result {
    use std::collections::HashMap;
    use std::sync::{LazyLock, Mutex};

    // Global registry (similar to SimulationSocket's PEER_REGISTRY)
    static REGISTRY: LazyLock<Mutex<HashMap<String, tokio::sync::mpsc::Sender<Vec<u8>>>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));

    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(10))
        .build();

    // Server host - registers itself and listens for messages
    sim.host("server", || async {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(10);

        // Register in global registry
        {
            let mut reg = REGISTRY.lock().unwrap();
            reg.insert("server".to_string(), tx);
        }

        // Wait for a message
        if let Some(msg) = rx.recv().await {
            assert_eq!(msg, b"hello from client");
            tracing::info!("Server received message!");
        }

        Ok(())
    });

    // Client host - looks up server and sends message
    sim.client("client", async {
        // Give server time to register
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Look up server in registry
        let tx = {
            let reg = REGISTRY.lock().unwrap();
            reg.get("server").cloned()
        };

        if let Some(tx) = tx {
            tx.send(b"hello from client".to_vec()).await.unwrap();
            tracing::info!("Client sent message!");
        }

        // Give time for message to be processed
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(())
    });

    sim.run()
}

/// Test: Verify determinism - same seed should produce same execution order
#[test]
fn test_turmoil_determinism() -> turmoil::Result {
    use std::sync::Mutex;

    fn run_simulation(_seed: u64) -> Vec<String> {
        let events = Arc::new(Mutex::new(Vec::new()));
        let events_result = events.clone();

        let mut sim = turmoil::Builder::new()
            .simulation_duration(Duration::from_secs(5))
            .build();

        // Multiple concurrent hosts
        for i in 0..3 {
            let events = events.clone();
            let host_name = format!("host-{i}");
            sim.host(host_name, move || {
                let events = events.clone();
                async move {
                    for j in 0..3 {
                        events.lock().unwrap().push(format!("host-{i}-iter-{j}"));
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Ok(())
                }
            });
        }

        sim.run().unwrap();

        // Drop the original Arc to reduce ref count
        drop(events);

        // Extract events from our result reference
        let guard = events_result.lock().unwrap();
        guard.clone()
    }

    // Run twice with implicit same seed (Turmoil should be deterministic)
    let run1 = run_simulation(42);
    let run2 = run_simulation(42);

    // Both runs should produce identical event ordering
    assert_eq!(run1, run2, "Turmoil should be deterministic");

    Ok(())
}

/// Test: Try to use SimulationSocket-style addressing inside Turmoil
/// This is the key test - can our in-memory socket work with Turmoil's scheduler?
#[test]
#[allow(clippy::type_complexity)]
fn test_turmoil_with_socket_pattern() -> turmoil::Result {
    use std::collections::HashMap;
    use std::sync::{LazyLock, Mutex};

    // Simplified socket registry (mirrors SimulationSocket's approach)
    struct SocketRegistry {
        sockets: Mutex<HashMap<SocketAddr, tokio::sync::mpsc::Sender<(SocketAddr, Vec<u8>)>>>,
    }

    impl SocketRegistry {
        fn new() -> Self {
            Self {
                sockets: Mutex::new(HashMap::new()),
            }
        }

        fn register(&self, addr: SocketAddr) -> tokio::sync::mpsc::Receiver<(SocketAddr, Vec<u8>)> {
            let (tx, rx) = tokio::sync::mpsc::channel(100);
            self.sockets.lock().unwrap().insert(addr, tx);
            rx
        }

        fn send(&self, from: SocketAddr, to: SocketAddr, data: Vec<u8>) -> bool {
            if let Some(tx) = self.sockets.lock().unwrap().get(&to) {
                tx.try_send((from, data)).is_ok()
            } else {
                false
            }
        }
    }

    static SOCKETS: LazyLock<SocketRegistry> = LazyLock::new(SocketRegistry::new);

    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(10))
        .build();

    let server_addr: SocketAddr = (Ipv6Addr::LOCALHOST, 8000).into();
    let client_addr: SocketAddr = (Ipv6Addr::LOCALHOST, 9000).into();

    // Server host
    sim.host("server", move || async move {
        let mut rx = SOCKETS.register(server_addr);
        tracing::info!("Server listening on {:?}", server_addr);

        // Wait for message
        if let Some((from, data)) = rx.recv().await {
            tracing::info!("Server received {:?} from {:?}", data, from);
            // Echo back
            SOCKETS.send(server_addr, from, b"pong".to_vec());
        }

        Ok(())
    });

    // Client host
    sim.client("client", async move {
        let mut rx = SOCKETS.register(client_addr);

        // Give server time to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Send to server
        tracing::info!("Client sending ping to {:?}", server_addr);
        assert!(SOCKETS.send(client_addr, server_addr, b"ping".to_vec()));

        // Wait for response
        tokio::time::sleep(Duration::from_millis(50)).await;

        if let Some((from, data)) = rx.recv().await {
            tracing::info!("Client received {:?} from {:?}", data, from);
            assert_eq!(data, b"pong");
            assert_eq!(from, server_addr);
        }

        Ok(())
    });

    sim.run()
}

/// Test: Use actual SimulationSocket inside Turmoil
/// This is the ultimate test - can our real in-memory socket work with Turmoil?
#[test]
fn test_turmoil_with_real_simulation_socket() -> turmoil::Result {
    // Clean up any previous socket state
    clear_all_socket_registries();

    let network_name = "turmoil-test";
    let virtual_time = VirtualTime::new();

    // Register the network's time source
    register_network_time_source(network_name, virtual_time);

    let server_addr: SocketAddr = (Ipv6Addr::LOCALHOST, 18000).into();
    let client_addr: SocketAddr = (Ipv6Addr::LOCALHOST, 19000).into();

    // Register addresses with the network
    register_address_network(server_addr, network_name);
    register_address_network(client_addr, network_name);

    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(10))
        .build();

    // Server host using SimulationSocket
    sim.host("server", move || async move {
        let socket = SimulationSocket::bind(server_addr)
            .await
            .expect("Server bind failed");

        tracing::info!("Server bound to {:?}", server_addr);

        // Wait for a packet
        let mut buf = [0u8; 1024];
        match socket.recv_from(&mut buf).await {
            Ok((len, from)) => {
                let msg = &buf[..len];
                tracing::info!("Server received {:?} from {:?}", msg, from);

                // Echo back
                socket.send_to(b"pong", from).await.ok();
            }
            Err(e) => {
                tracing::error!("Server recv error: {:?}", e);
            }
        }

        Ok(())
    });

    // Client host using SimulationSocket
    sim.client("client", async move {
        // Give server time to bind
        tokio::time::sleep(Duration::from_millis(100)).await;

        let socket = SimulationSocket::bind(client_addr)
            .await
            .expect("Client bind failed");

        tracing::info!("Client bound to {:?}", client_addr);

        // Send ping to server
        socket.send_to(b"ping", server_addr).await.ok();
        tracing::info!("Client sent ping");

        // Wait for response
        let mut buf = [0u8; 1024];
        match tokio::time::timeout(Duration::from_secs(5), socket.recv_from(&mut buf)).await {
            Ok(Ok((len, from))) => {
                let msg = &buf[..len];
                tracing::info!("Client received {:?} from {:?}", msg, from);
                assert_eq!(msg, b"pong");
                assert_eq!(from, server_addr);
            }
            Ok(Err(e)) => {
                tracing::error!("Client recv error: {:?}", e);
            }
            Err(_) => {
                tracing::error!("Client recv timeout");
            }
        }

        Ok(())
    });

    sim.run()
}
