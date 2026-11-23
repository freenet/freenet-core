use super::*;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

const MIB: usize = 1024 * 1024;
const GIB: usize = 1024 * MIB;

fn v4(last: u8, port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, last)), port)
}

/// G = 400, P = 100, reserve = 100: sources holding something share 300.
/// The oversize slot is off, so the ordinary rules are what is tested.
fn small() -> Arc<OutboundPayloadBudget> {
    let b = OutboundPayloadBudget::with_caps(400, 100);
    b.set_oversize_max_for_test(0);
    b
}

fn key(n: u8) -> SharedPayloadKey {
    SharedPayloadKey::for_test(n)
}

/// A charge for a response no other stream shares: every call uses a key
/// no earlier call used, so the node is charged the full length.
trait ReserveUnshared {
    fn try_reserve(&self, to: SocketAddr, len: u64) -> Result<bytes::Bytes, Refusal>;
}

impl ReserveUnshared for Arc<OutboundPayloadBudget> {
    fn try_reserve(&self, to: SocketAddr, len: u64) -> Result<bytes::Bytes, Refusal> {
        use std::sync::atomic::AtomicU64;
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let mut k = key(0);
        k.state_hash[..8].copy_from_slice(&n.to_le_bytes());
        shared(self, to, k, len as usize)
    }
}

fn shared(
    budget: &Arc<OutboundPayloadBudget>,
    to: SocketAddr,
    k: SharedPayloadKey,
    len: usize,
) -> Result<bytes::Bytes, Refusal> {
    budget
        .try_reserve_shared(to, k, len as u64, || {
            Ok::<_, std::convert::Infallible>(vec![7; len])
        })
        .map_err(|e| match e {
            SharedReserveError::Refused(r) => r,
            SharedReserveError::Serialize(never) => match never {},
            SharedReserveError::LengthMismatch { .. } => panic!("length mismatch"),
        })
}

#[test]
fn budget_is_a_sixteenth_of_memory_clamped() {
    assert_eq!(outbound_payload_budget_for(256 * MIB), 64 * MIB, "floor");
    assert_eq!(outbound_payload_budget_for(GIB), 64 * MIB);
    assert_eq!(outbound_payload_budget_for(2 * GIB), 128 * MIB);
    assert_eq!(outbound_payload_budget_for(4 * GIB), 256 * MIB);
    assert_eq!(outbound_payload_budget_for(7_600 * MIB), 475 * MIB);
    assert_eq!(outbound_payload_budget_for(64 * GIB), 512 * MIB, "ceiling");
    let b = OutboundPayloadBudget::for_memory_limit(4 * GIB);
    assert_eq!(b.per_source_cap(), 64 * MIB as u64);
}

/// Repeated requests from one source: whatever their number and whichever
/// port they come from, the source holds at most P.
#[test]
fn one_source_cannot_hold_more_than_its_cap() {
    let b = small();
    let held: Vec<_> = (0..20)
        .filter_map(|i| b.try_reserve(v4(1, 1000 + i), 30).ok())
        .collect();
    assert_eq!(held.len(), 3, "3 x 30 fits P = 100, a fourth does not");
    assert_eq!(b.charged_to(v4(1, 1)), 90);
    assert_eq!(b.try_reserve(v4(1, 9), 30).err(), Some(Refusal::SourceCap));
    // Another source is unaffected.
    assert!(b.try_reserve(v4(2, 1), 30).is_ok());
}

/// IPv6 addresses in one /64 are one source.
#[test]
fn an_ipv6_slash_64_is_one_source() {
    let b = small();
    let a = SocketAddr::new(
        IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 1)),
        1,
    );
    let c = SocketAddr::new(
        IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 9, 9, 9, 9)),
        2,
    );
    let _first = b.try_reserve(a, 80).unwrap();
    assert_eq!(b.try_reserve(c, 30).err(), Some(Refusal::SourceCap));
}

/// Sources that already hold responses share G - reserve; the reserve stays
/// available to a source holding nothing.
#[test]
fn busy_sources_cannot_take_the_reserve() {
    let b = small();
    // Each source takes a first response (idle sources may use all of G),
    // then a second one, which must fit within G - reserve = 300.
    let mut held = Vec::new();
    for s in 1..=3 {
        held.push(b.try_reserve(v4(s, 1), 50).unwrap());
    }
    assert_eq!(b.node_total(), 150);
    for s in 1..=3 {
        held.push(b.try_reserve(v4(s, 1), 50).unwrap());
    }
    assert_eq!(b.node_total(), 300);
    // Sources 1-3 hold their whole P. Source 4 takes a first response from
    // the reserve (idle), and its second is refused by G - reserve, not by
    // its own cap.
    held.push(b.try_reserve(v4(4, 1), 10).unwrap());
    assert_eq!(b.try_reserve(v4(4, 1), 10).err(), Some(Refusal::NodeCap));
    // An idle source still gets in from the reserve.
    assert!(b.try_reserve(v4(5, 1), 80).is_ok());
}

/// A response larger than P is admitted only when its source holds nothing.
#[test]
fn oversize_only_when_the_source_is_idle() {
    let b = small();
    let big = b
        .try_reserve(v4(1, 1), 250)
        .expect("idle source, room in G");
    assert_eq!(b.try_reserve(v4(1, 1), 1).err(), Some(Refusal::SourceCap));
    drop(big);
    let _small = b.try_reserve(v4(1, 1), 1).unwrap();
    assert_eq!(b.try_reserve(v4(1, 1), 250).err(), Some(Refusal::SourceCap));
    // Never more than G, even for an idle source.
    assert_eq!(b.try_reserve(v4(2, 1), 400).err(), Some(Refusal::NodeCap));
    assert!(b.try_reserve(v4(2, 1), 399).is_ok());
}

/// A refusal charges nothing and leaves no entry.
#[test]
fn a_refusal_charges_nothing() {
    let b = small();
    let _a = b.try_reserve(v4(1, 1), 100).unwrap();
    assert!(b.try_reserve(v4(1, 1), 1).is_err());
    assert_eq!(b.node_total(), 100);
    assert_eq!(b.charged_to(v4(1, 1)), 100);
    assert_eq!(b.refused_totals(), (1, 0));
}

/// The charge lasts exactly as long as any slice of the bytes.
#[test]
fn the_charge_is_released_with_the_last_slice() {
    let b = small();
    let mut data = b.try_reserve(v4(1, 1), 100).unwrap();
    let fragment = data.split_to(10);
    drop(data);
    assert_eq!(b.node_total(), 100, "a fragment is still held");
    assert_eq!(b.charged_to(v4(1, 1)), 100);
    drop(fragment);
    assert_eq!(b.node_total(), 0);
    assert_eq!(b.charged_to(v4(1, 1)), 0);
    // An unused reservation is released too.
    drop(b.try_reserve(v4(1, 1), 100).unwrap());
    assert_eq!(b.node_total(), 0);
}

/// Concurrent streams of the same response share one copy: the node is
/// charged once, each source per stream, and the copy (with its table
/// entry) goes with the last stream.
#[test]
fn streams_of_one_response_share_one_copy() {
    let b = small();
    let k = key(1);
    let serialized = std::cell::Cell::new(0);
    let serialize = || {
        serialized.set(serialized.get() + 1);
        Ok::<_, ()>(vec![3; 60])
    };
    let s1 = b
        .try_reserve_shared(v4(1, 1), k, 60, serialize)
        .ok()
        .unwrap();
    let s2 = b
        .try_reserve_shared(v4(2, 1), k, 60, serialize)
        .ok()
        .unwrap();
    let s3 = b
        .try_reserve_shared(v4(3, 1), k, 60, serialize)
        .ok()
        .unwrap();
    assert_eq!(serialized.get(), 1, "one serialization for three streams");
    assert_eq!(b.shared_strong_count(&k), 3);
    assert_eq!(b.node_total(), 60, "one copy counted once");
    assert_eq!(b.charged_to(v4(2, 1)), 60, "each source charged per stream");
    assert_eq!(s1.as_ptr(), s2.as_ptr(), "same bytes");
    assert_eq!(&s3[..], &[3; 60][..]);
    drop((s1, s2));
    assert_eq!(b.shared_strong_count(&k), 1);
    drop(s3);
    assert_eq!(b.node_total(), 0);
    assert_eq!(b.shared_entries(), 0, "dead entries are pruned");
    // A later request serializes afresh.
    let _s4 = b
        .try_reserve_shared(v4(1, 1), k, 60, serialize)
        .ok()
        .unwrap();
    assert_eq!(serialized.get(), 2);
}

/// Sharing does not lift the per-source cap: one source cannot open more
/// than P of streams even when they all share a copy.
#[test]
fn a_shared_copy_still_charges_each_stream_to_its_source() {
    let b = small();
    let k = key(2);
    let _a = shared(&b, v4(1, 1), k, 40).unwrap();
    let _b = shared(&b, v4(1, 2), k, 40).unwrap();
    assert_eq!(shared(&b, v4(1, 3), k, 40).err(), Some(Refusal::SourceCap));
    assert_eq!(b.node_total(), 40);
}

/// Different responses (another state, or with and without code) are
/// separate copies.
#[test]
fn different_responses_do_not_share() {
    let b = small();
    let mut k2 = key(3);
    let _a = shared(&b, v4(1, 1), k2, 40).unwrap();
    k2.includes_contract = false;
    let _b = shared(&b, v4(2, 1), k2, 40).unwrap();
    assert_eq!(b.shared_copies(), 2);
    assert_eq!(b.node_total(), 80);
}

/// A stream of an existing copy adds no node memory, so it is admitted even
/// when the node is past G - reserve; a new copy is not.
#[test]
fn a_shared_stream_needs_no_node_room() {
    let b = small();
    let k = key(4);
    let _first = shared(&b, v4(1, 1), k, 90).unwrap();
    let _fill: Vec<_> = (2..=4)
        .map(|s| b.try_reserve(v4(s, 1), 70).unwrap())
        .collect();
    assert_eq!(b.node_total(), 300);
    let _held = b.try_reserve(v4(5, 1), 1).unwrap();
    // Source 5 is busy and the node is past 300: a new copy is refused...
    assert_eq!(
        shared(&b, v4(5, 1), key(5), 10).err(),
        Some(Refusal::NodeCap)
    );
    // ...but a stream of the copy already held is not.
    assert!(shared(&b, v4(5, 1), k, 90).is_ok());
}

/// A refused or failed shared reservation does not serialize, or gives its
/// charges back.
#[test]
fn a_refused_shared_reservation_does_not_serialize() {
    let b = small();
    let _full = b.try_reserve(v4(1, 1), 100).unwrap();
    let mut called = false;
    let r = b.try_reserve_shared(v4(1, 1), key(6), 10, || {
        called = true;
        Ok::<_, ()>(vec![0; 10])
    });
    assert!(matches!(
        r,
        Err(SharedReserveError::Refused(Refusal::SourceCap))
    ));
    assert!(!called);
    let r = b.try_reserve_shared(v4(2, 1), key(6), 10, || Err::<Vec<u8>, _>("boom"));
    assert!(matches!(r, Err(SharedReserveError::Serialize("boom"))));
    assert_eq!(b.node_total(), 100);
    assert_eq!(b.charged_to(v4(2, 1)), 0);
}

/// Two sources each saturating their cap leave a third served (two peers
/// that fill their shares do not fill the node).
#[test]
fn two_saturated_sources_leave_a_third_served() {
    let b = OutboundPayloadBudget::for_memory_limit(GIB);
    let p = b.per_source_cap();
    let size = 8 * MIB as u64;
    let mut held = Vec::new();
    for s in 1..=2 {
        while let Ok(r) = b.try_reserve(v4(s, 1), size) {
            held.push(r);
        }
        assert!(b.charged_to(v4(s, 1)) <= p);
    }
    assert!(b.try_reserve(v4(3, 1), size).is_ok());
    // Once those transfers end, a largest-state response to an idle fourth
    // source fits even on the smallest budget (G = 64 MiB, P = 16 MiB).
    drop(held);
    let largest = crate::wasm_runtime::MAX_STATE_SIZE as u64;
    assert!(largest > p, "the oversize rule is what admits it");
    assert!(b.try_reserve(v4(4, 1), largest).is_ok());
}

fn v6(prefix_last: u16, host: u16) -> SocketAddr {
    SocketAddr::new(
        IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, prefix_last, 0, 0, 0, host)),
        1,
    )
}

/// Different /64 prefixes, and different IPv4 addresses, are different
/// sources: each has its own cap.
#[test]
fn different_prefixes_and_addresses_are_different_sources() {
    let b = small();
    let _a = b.try_reserve(v6(1, 1), 100).unwrap();
    assert_eq!(b.try_reserve(v6(1, 2), 1).err(), Some(Refusal::SourceCap));
    let _b = b.try_reserve(v6(2, 1), 100).expect("another /64");
    let _c = b.try_reserve(v4(1, 1), 100).expect("an IPv4 source");
    let _d = b.try_reserve(v4(2, 1), 100).expect("another IPv4 address");
    assert_eq!(b.charged_to(v6(1, 1)), 100);
    assert_eq!(b.charged_to(v6(2, 1)), 100);
    assert_eq!(b.charged_to(v4(1, 1)), 100);
}

/// At the smallest node budget (G = 64 MiB, P = 16 MiB) with two other
/// sources at their caps, a maximum-size response (state plus code and
/// parameters) is still served to an idle source, one at a time, and the
/// node never holds more than G plus one oversize response.
#[test]
fn a_maximum_size_response_is_served_at_the_smallest_budget() {
    let b = OutboundPayloadBudget::for_memory_limit(GIB);
    assert_eq!(b.per_source_cap(), (16 * MIB) as u64);
    let mut held = Vec::new();
    for s in 1..=3 {
        // 16 MiB each in 4 MiB responses: three sources at their caps.
        for _ in 0..4 {
            held.push(b.try_reserve(v4(s, 1), 4 * MIB as u64).unwrap());
        }
        assert_eq!(b.charged_to(v4(s, 1)), (16 * MIB) as u64);
    }
    // 48 MiB held; the reserve (16 MiB) is smaller than a 50 MiB state.
    let max_state = crate::wasm_runtime::MAX_STATE_SIZE as u64;
    let with_code = max_state + 20 * MIB as u64;
    let big = b
        .try_reserve(v4(4, 1), with_code)
        .expect("an idle source gets the oversize slot");
    assert!(b.node_total() <= (64 * MIB) as u64 + OVERSIZE_RESPONSE_MAX_BYTES);
    // Only one oversize response at a time, even to another idle source.
    assert_eq!(
        b.try_reserve(v4(5, 1), max_state).err(),
        Some(Refusal::NodeCap)
    );
    // And never to a source that holds something.
    assert!(b.try_reserve(v4(1, 1), max_state).is_err());
    // Larger than the slot allows is refused.
    drop(big);
    assert_eq!(
        b.try_reserve(v4(5, 1), OVERSIZE_RESPONSE_MAX_BYTES + 1)
            .err(),
        Some(Refusal::NodeCap)
    );
    // The slot is free again once the response is gone.
    let again = b.try_reserve(v4(5, 1), max_state).expect("slot returned");
    drop(again);
    drop(held);
    assert_eq!(b.node_total(), 0);
}

/// A response larger than G itself is servable on an otherwise idle node.
#[test]
fn a_response_larger_than_the_whole_budget_uses_the_oversize_slot() {
    let b = OutboundPayloadBudget::for_memory_limit(GIB);
    let r = b.try_reserve(v4(1, 1), (100 * MIB) as u64).unwrap();
    assert_eq!(b.node_total(), (100 * MIB) as u64);
    drop(r);
    assert_eq!(b.node_total(), 0);
}

/// A serializer that produces other than the length charged is an error and
/// leaves nothing charged or published.
#[test]
fn a_serializer_that_disagrees_with_the_length_is_refused() {
    let b = small();
    for produced in [9usize, 11] {
        let r = b.try_reserve_shared(v4(1, 1), key(1), 10, || {
            Ok::<_, std::convert::Infallible>(vec![0; produced])
        });
        assert!(matches!(
            r,
            Err(SharedReserveError::LengthMismatch { charged: 10, .. })
        ));
        assert_eq!(b.node_total(), 0);
        assert_eq!(b.charged_to(v4(1, 1)), 0);
        assert_eq!(b.shared_entries(), 0);
    }
}

/// A live copy of another length at the same key is dropped outside the
/// budget's lock: when the last stream of that copy releases it at the same
/// moment, the copy's `Drop` takes the lock, and holding it here deadlocked.
#[test]
fn replacing_a_mismatched_copy_does_not_deadlock_with_its_release() {
    use std::sync::mpsc;
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let b = OutboundPayloadBudget::with_caps(1 << 30, 1 << 29);
        for round in 0..3000u64 {
            let old = shared(&b, v4(1, 1), key(7), 10).unwrap();
            let b2 = b.clone();
            let releaser = std::thread::spawn(move || drop(old));
            // Another length for the same key: the existing copy is found
            // and rejected while the releaser may be dropping the last
            // other reference.
            let new = shared(&b2, v4(2, 1), key(7), 11 + (round % 2) as usize).unwrap();
            releaser.join().unwrap();
            drop(new);
        }
        assert_eq!(b.node_total(), 0);
        done_tx.send(()).unwrap();
    });
    done_rx
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("no deadlock between a lookup and a release");
}

/// Refusals are counted process-wide for telemetry as well as per budget.
#[test]
fn refusals_reach_the_process_wide_counters() {
    let (s0, _) = refused_totals_process();
    let b = small();
    let _a = b.try_reserve(v4(1, 1), 100).unwrap();
    assert_eq!(b.try_reserve(v4(1, 1), 1).err(), Some(Refusal::SourceCap));
    let (s1, n1) = refused_totals_process();
    assert!(s1 > s0, "per-source refusals are exported");
    // A node-wide refusal moves the other counter. Only increases are
    // asserted: the totals are process-global, so other tests in the same
    // process add to them too.
    assert_eq!(b.try_reserve(v4(2, 1), 400).err(), Some(Refusal::NodeCap));
    let (_, n2) = refused_totals_process();
    assert!(n2 > n1, "node-wide refusals are exported");
    assert_eq!(b.refused_totals(), (1, 1), "this budget's own counts");
}

/// The oversize slot goes only to a source holding nothing: a source holding
/// something, under P, is refused when the node has no room.
#[test]
fn the_oversize_slot_needs_an_idle_source() {
    // G = 400, P = 100, oversize slot on (default maximum).
    let b = OutboundPayloadBudget::with_caps(400, 100);
    let _held: Vec<_> = (2..=4)
        .map(|s| b.try_reserve(v4(s, 1), 100).unwrap())
        .collect();
    let _small = b.try_reserve(v4(1, 1), 1).unwrap();
    assert_eq!(b.node_total(), 301);
    assert_eq!(
        b.try_reserve(v4(1, 1), 50).err(),
        Some(Refusal::NodeCap),
        "holding something, so not eligible for the slot"
    );
    // An idle source asking for more than the node has room for does take it.
    assert!(b.try_reserve(v4(5, 1), 150).is_ok());
}

/// A copy that holds the oversize slot is not joined by later streams.
#[test]
fn an_oversize_copy_is_not_joined() {
    let b = OutboundPayloadBudget::with_caps(400, 100);
    let first = shared(&b, v4(1, 1), key(1), 500).expect("oversize slot");
    assert_eq!(
        shared(&b, v4(2, 1), key(1), 500).err(),
        Some(Refusal::NodeCap),
        "another stream of the same oversize response is refused, not joined"
    );
    drop(first);
    assert_eq!(b.node_total(), 0);
    let again = shared(&b, v4(2, 1), key(1), 500).expect("slot free again");
    drop(again);
}

/// Refusal on length alone, before the caller hashes the state.
#[test]
fn precheck_refuses_without_a_copy_and_counts() {
    let b = small();
    let k = key(3);
    assert_eq!(
        b.precheck(v4(1, 1), k.contract, true, 401).err(),
        Some(Refusal::NodeCap)
    );
    assert!(b.precheck(v4(1, 1), k.contract, true, 100).is_ok());
    let _held: Vec<_> = (2..=4)
        .map(|s| b.try_reserve(v4(s, 1), 100).unwrap())
        .collect();
    // The node has no room for another copy of this contract's response,
    // but a live copy of it makes a stream of it free for the node.
    let copy = shared(&b, v4(1, 1), k, 90).unwrap();
    assert!(b.precheck(v4(5, 1), k.contract, true, 90).is_ok());
    assert_eq!(
        b.precheck(v4(5, 1), key(9).contract, true, 90).err(),
        Some(Refusal::NodeCap)
    );
    drop(copy);
}

/// A copy of another length published while a reservation is serializing is
/// dropped outside the lock (`publish_copy`), like the one found at the
/// first lookup.
#[test]
fn publishing_beside_a_mismatched_copy_does_not_deadlock_with_its_release() {
    use std::sync::mpsc;
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let b = OutboundPayloadBudget::with_caps(1 << 30, 1 << 29);
        for _ in 0..3000u64 {
            let b2 = b.clone();
            let (to_release, release) = mpsc::channel();
            let releaser = std::thread::spawn(move || {
                if let Ok(copy) = release.recv() {
                    drop(copy);
                }
            });
            // While this reservation serializes, a copy of another length
            // appears for the same key; the releaser drops it concurrently
            // with this reservation's publish.
            let r = b.try_reserve_shared(v4(1, 1), key(7), 10, || {
                let other = shared(&b2, v4(2, 1), key(7), 11).unwrap();
                to_release.send(other).unwrap();
                Ok::<_, std::convert::Infallible>(vec![0; 10])
            });
            drop(to_release);
            releaser.join().unwrap();
            drop(r);
        }
        assert_eq!(b.node_total(), 0);
        done_tx.send(()).unwrap();
    });
    done_rx
        .recv_timeout(std::time::Duration::from_secs(60))
        .expect("no deadlock between a publish and a release");
}

/// `precheck` applies the test `try_reserve_shared` does to a live copy: the
/// same length and not holding the oversize slot. A copy that cannot be
/// joined does not let a request through to be hashed and then refused.
#[test]
fn precheck_does_not_count_a_copy_that_cannot_be_joined() {
    let b = OutboundPayloadBudget::with_caps(400, 100);
    let k = key(4);
    let _oversize = shared(&b, v4(1, 1), k, 500).expect("oversize slot");
    // Another source, same contract and length: the live copy is oversize,
    // so it is not joined and the node has no room for another.
    assert_eq!(
        b.precheck(v4(2, 1), k.contract, true, 500).err(),
        Some(Refusal::NodeCap)
    );
    // A live copy of another length is not this response either.
    let b = small();
    let _copy = shared(&b, v4(1, 1), k, 90).unwrap();
    let _held: Vec<_> = (2..=4)
        .map(|s| b.try_reserve(v4(s, 1), 100).unwrap())
        .collect();
    assert_eq!(
        b.precheck(v4(5, 1), k.contract, true, 91).err(),
        Some(Refusal::NodeCap),
        "a copy of another length is not joinable"
    );
    assert!(b.precheck(v4(5, 1), k.contract, true, 90).is_ok());
}
