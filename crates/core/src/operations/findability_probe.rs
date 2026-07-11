//! EXPERIMENT-ONLY findability instrumentation (NOT for ship).
//!
//! Two decisive-isolation questions the demand-driven hosting epic needs
//! answered (findability is invariant 5, the epic's core goal):
//!
//!  1. PUT-reach: does a scatter-free PUT from a far node land its single copy
//!     on the peer CLOSEST to the key (rank 0), or does it stop short?
//!  2. GET-reach: does a distant GET reach the closest peer where a correctly
//!     placed copy lives, or does it dead-end elsewhere?
//!
//! And for every miss, the WHY — a **connectivity gap** (the terminus has no
//! CONNECTED neighbor closer to the key) vs a **routing give-up** (a closer
//! neighbor exists but HTL / the one-retry relay cap / the visited filter / the
//! #4363 terminus guard cut the walk off anyway).
//!
//! Two thread-local hooks, flipped per experiment arm and reset by the driver:
//!
//!  * [`set_scatter_disabled`] — when on, the every-hop PUT store
//!    (`relay_put_store_locally`), the terminus PUT replication
//!    (`relay_put_replicate_forward`) and the GET-return-path relay cache
//!    (`cache_contract_locally`, relay hops only) become no-ops, so the seeded
//!    or PUT copy stays SINGULAR and cannot self-scatter (the confound that
//!    invalidated an earlier run). The PUT terminus still stores its one copy
//!    explicitly (see `relay_put_finalize_scatter_disabled_store` in the PUT
//!    driver) so exactly one holder lands at the terminus.
//!  * [`take_op_traces`] — drains the per-op terminus records the drivers push
//!    at each GET/PUT terminus (rank, HTL, hop count, stop reason, and the KEY
//!    connectivity-gap-vs-give-up field).
//!
//! THREAD-LOCAL on purpose: the sim harness runs every node on the test's own
//! current-thread runtime and relies on thread-local globals to stay
//! parallel-test-safe (see `connection_manager::NN_NEAREST_EDGE_CLAUSE`). Do NOT
//! wire any of this into production config.

use std::cell::{Cell, RefCell};
use std::net::SocketAddr;

use crate::ring::{Location, Ring};

thread_local! {
    static SCATTER_DISABLED: Cell<bool> = const { Cell::new(false) };
    static OP_TRACES: RefCell<Vec<OpTraceRecord>> = const { RefCell::new(Vec::new()) };
}

/// Which operation produced a terminus record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProbeOpKind {
    Put,
    Get,
}

/// Why routing stopped at the recorded terminus.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProbeStopReason {
    /// GET: the copy was found locally at this peer (local cache hit w/ interest
    /// or local fallback). For the single-seeded-copy experiment this fires only
    /// at the rank-0 holder, so it means "greedy routing reached the copy".
    Found,
    /// GET/PUT: HTL reached 0 at this peer.
    HtlZero,
    /// GET: no unvisited routing candidate remained (the one-retry relay cap
    /// plus the visited filter exhausted this relay's forwardable peers).
    RoutingExhausted,
    /// PUT: #4363 terminus guard fired — the chain descended to this peer and
    /// the only forwardable next hop moves away from the key.
    TerminusGuard,
    /// PUT: no next-hop candidate at all (and no bootstrap gateway) — this peer
    /// is the final destination.
    NoNextHop,
}

/// One terminus observation. All nodes in a sim run on one OS thread, so a
/// single thread-local vec collects records from every peer; each record's
/// `own_loc` lets the test rank it post-hoc.
#[derive(Clone, Debug)]
pub struct OpTraceRecord {
    pub kind: ProbeOpKind,
    /// Attempt transaction id (groups a GET relay chain's termini).
    pub tx: String,
    /// The terminus peer's own ring location.
    pub own_loc: f64,
    /// The contract key's ring location (the routing target).
    pub target_loc: f64,
    /// Ring distance from the terminus peer to the key.
    pub own_dist: f64,
    pub htl_remaining: usize,
    pub hop_count: usize,
    pub stop_reason: ProbeStopReason,
    /// Ring location of the terminus peer's CLOSEST connected neighbor that is
    /// strictly closer to the key than the terminus itself. `None` = the
    /// terminus has NO closer connected neighbor = a **connectivity gap**.
    /// `Some(_)` = a **routing give-up** (a closer neighbor existed).
    pub closer_neighbor_loc: Option<f64>,
    /// Ring distance-to-key of that closest-closer neighbor (if any).
    pub closer_neighbor_dist: Option<f64>,
    /// Whether that closest-closer neighbor was already visited / skip-listed
    /// (so the give-up is specifically a **visited-filter** give-up rather than
    /// a router/next-hop-selection or retry-cap give-up).
    pub closer_neighbor_visited: Option<bool>,
    /// Total connected neighbors of the terminus peer.
    pub num_neighbors: usize,
}

/// Enable/disable the scatter/cache no-op hooks for the CURRENT THREAD.
pub fn set_scatter_disabled(enabled: bool) {
    SCATTER_DISABLED.with(|c| c.set(enabled));
}

/// Read the scatter-disable flag for the current thread.
pub fn scatter_disabled() -> bool {
    SCATTER_DISABLED.with(|c| c.get())
}

/// Drop any buffered op traces for the current thread (call before a run).
pub fn clear_op_traces() {
    OP_TRACES.with(|t| t.borrow_mut().clear());
}

/// Drain the buffered op traces for the current thread (call after a run).
pub fn take_op_traces() -> Vec<OpTraceRecord> {
    OP_TRACES.with(|t| std::mem::take(&mut *t.borrow_mut()))
}

fn push_trace(record: OpTraceRecord) {
    OP_TRACES.with(|t| t.borrow_mut().push(record));
}

fn ring_dist(a: f64, b: f64) -> f64 {
    let d = (a - b).abs();
    d.min(1.0 - d)
}

/// Record a terminus observation, computing the closer-neighbor field from the
/// live connection manager. `is_visited(addr)` reports whether a neighbor was
/// already visited / skip-listed on this op (GET: tried ∪ visited-bloom; PUT:
/// skip_list). A no-op if `own_location` is unknown (can't rank).
#[allow(clippy::too_many_arguments)]
pub(crate) fn record_terminus(
    ring: &Ring,
    kind: ProbeOpKind,
    tx: &crate::message::Transaction,
    target: Location,
    htl_remaining: usize,
    hop_count: usize,
    stop_reason: ProbeStopReason,
    is_visited: impl Fn(SocketAddr) -> bool,
) {
    let cm = &ring.connection_manager;
    let Some(own_loc) = cm.own_location().location() else {
        return;
    };
    let own_f = own_loc.as_f64();
    let target_f = target.as_f64();
    let own_dist = ring_dist(own_f, target_f);

    let mut num_neighbors = 0usize;
    // (dist, loc, visited) of the closest connected neighbor strictly closer.
    let mut closest_closer: Option<(f64, f64, bool)> = None;
    for (loc, conns) in cm.get_connections_by_location().iter() {
        let nb_f = loc.as_f64();
        let nb_dist = ring_dist(nb_f, target_f);
        for conn in conns {
            num_neighbors += 1;
            if nb_dist < own_dist {
                let visited = conn
                    .location
                    .socket_addr()
                    .map(&is_visited)
                    .unwrap_or(false);
                let better = match &closest_closer {
                    Some((d, _, _)) => nb_dist < *d,
                    None => true,
                };
                if better {
                    closest_closer = Some((nb_dist, nb_f, visited));
                }
            }
        }
    }

    let (closer_neighbor_loc, closer_neighbor_dist, closer_neighbor_visited) = match closest_closer
    {
        Some((d, l, v)) => (Some(l), Some(d), Some(v)),
        None => (None, None, None),
    };

    push_trace(OpTraceRecord {
        kind,
        tx: tx.to_string(),
        own_loc: own_f,
        target_loc: target_f,
        own_dist,
        htl_remaining,
        hop_count,
        stop_reason,
        closer_neighbor_loc,
        closer_neighbor_dist,
        closer_neighbor_visited,
        num_neighbors,
    });
}
