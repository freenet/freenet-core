#[test]
fn test() {
    println!("hello world!");
}

enum Instructions {
    GetPeerStats(PeerStats),
}

struct PeerStats {}
