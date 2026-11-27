//! One accountant for the memory that compiles may take beside the node.
//!
//! Every compile that is bounded by the worker machinery (a worker, or an
//! in-process compile of a large module the worker already accepted) holds a
//! reservation against one shared budget for as long as it runs. A reservation is
//! granted only when the memory limit's usable share, less the node's resident
//! memory right now, less every reservation already held, still covers it. So the
//! sum of reservations plus the node's own memory cannot pass the usable share at
//! the moment any of them is granted, however many compiles are asked for and
//! however the node's memory moves in between. A reservation that is already held
//! is never shrunk when the node grows; new ones get less, or wait.

use std::sync::{Arc, Condvar, Mutex};
// The system clock is deliberate here, not `TimeSource`: `acquire` blocks a real
// thread on a real `Condvar` with a real timeout, so its deadline must be the
// clock that wait runs on. The reservation-wait cost is recorded by the caller
// in `isolation.rs` through the node's `TimeSource`.
use std::time::{Duration, Instant};

use super::caps::{self, Policy};

/// What the node's cgroup and process look like right now.
#[derive(Debug, Clone, Copy)]
pub(crate) struct HostLimits {
    pub memory_limit: u64,
    /// The node's current resident memory (not its lifetime peak: one spike must
    /// not shut the check down for the rest of the run).
    pub node_rss: u64,
    pub cores: Option<f64>,
    /// The process is in a cgroup whose memory limit could not be read.
    pub limit_unreadable: bool,
}

pub(crate) type LimitsSource = Arc<dyn Fn() -> HostLimits + Send + Sync>;

/// How often a waiter re-reads the node's memory while it waits.
const RECHECK: Duration = Duration::from_millis(500);

struct State {
    reserved: u64,
    workers: usize,
}

struct Shared {
    policy: Policy,
    limits: LimitsSource,
    max_workers: usize,
    state: Mutex<State>,
    freed: Condvar,
}

/// The shared accountant. Cloning shares it.
#[derive(Clone)]
pub(crate) struct MemoryBudget(Arc<Shared>);

/// Memory held against the budget; released on drop.
pub(crate) struct Reservation {
    budget: MemoryBudget,
    bytes: u64,
    worker: bool,
}

impl Reservation {
    /// The bytes held. For a worker this is its `RLIMIT_DATA`.
    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let mut st = self.budget.0.lock();
        st.reserved = st.reserved.saturating_sub(self.bytes);
        if self.worker {
            st.workers = st.workers.saturating_sub(1);
        }
        drop(st);
        self.budget.0.freed.notify_all();
    }
}

/// Why a reservation was not granted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Denied {
    /// The headroom under the limit cannot cover it: at once when nothing is
    /// running, else once the wait ended with the memory still short.
    TooSmall { available: u64 },
    /// The memory would cover it, but every worker slot stayed taken for the
    /// whole wait.
    TimedOut,
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl MemoryBudget {
    pub(crate) fn new(policy: Policy, max_workers: usize, limits: LimitsSource) -> Self {
        Self(Arc::new(Shared {
            policy,
            limits,
            max_workers: max_workers.max(1),
            state: Mutex::new(State {
                reserved: 0,
                workers: 0,
            }),
            freed: Condvar::new(),
        }))
    }

    /// The host limits now.
    pub(crate) fn host(&self) -> HostLimits {
        (self.0.limits)()
    }

    /// Bytes currently reserved.
    #[cfg(test)]
    pub(crate) fn reserved(&self) -> u64 {
        self.0.lock().reserved
    }

    /// The cap a worker would get right now, without reserving it. A cache key
    /// hint only: the grant is made by [`MemoryBudget::acquire_worker`].
    pub(crate) fn peek_worker_cap(&self) -> Option<u64> {
        let st = self.0.lock();
        let h = (self.0.limits)();
        caps::worker_cap(&self.0.policy, h.memory_limit, h.node_rss, st.reserved)
    }

    /// Reserve memory for one worker: all that is free, up to the ceiling, and at
    /// least the floor. At most `max_workers` hold one at a time.
    pub(crate) fn acquire_worker(&self, max_wait: Duration) -> Result<Reservation, Denied> {
        self.acquire(true, 0, max_wait)
    }

    /// Reserve exactly `bytes` for an in-process compile.
    pub(crate) fn acquire_in_process(
        &self,
        bytes: u64,
        max_wait: Duration,
    ) -> Result<Reservation, Denied> {
        self.acquire(false, bytes, max_wait)
    }

    fn acquire(&self, worker: bool, bytes: u64, max_wait: Duration) -> Result<Reservation, Denied> {
        let deadline = Instant::now() + max_wait;
        let shared = &self.0;
        let mut st = shared.lock();
        loop {
            // Read the host with the lock held, so the reservations compared
            // against it are the ones that exist when the grant is made.
            let h = (shared.limits)();
            let free = caps::available(h.memory_limit, h.node_rss, st.reserved);
            let granted = if worker {
                (st.workers < shared.max_workers)
                    .then(|| {
                        caps::worker_cap(&shared.policy, h.memory_limit, h.node_rss, st.reserved)
                    })
                    .flatten()
            } else {
                (free >= bytes).then_some(bytes)
            };
            if let Some(got) = granted {
                st.reserved += got;
                if worker {
                    st.workers += 1;
                }
                return Ok(Reservation {
                    budget: self.clone(),
                    bytes: got,
                    worker,
                });
            }
            let needed = if worker {
                shared.policy.data_floor
            } else {
                bytes
            };
            let nothing_running = st.reserved == 0 && st.workers == 0;
            if nothing_running && caps::available(h.memory_limit, h.node_rss, 0) < needed {
                return Err(Denied::TooSmall {
                    available: caps::available(h.memory_limit, h.node_rss, 0),
                });
            }
            let now = Instant::now();
            if now >= deadline {
                // Say which resource ran out: memory is a node-level condition
                // (reported as degraded), a full slot table is only load.
                let free_now = caps::available(h.memory_limit, h.node_rss, st.reserved);
                return Err(if free_now < needed {
                    Denied::TooSmall {
                        available: free_now,
                    }
                } else {
                    Denied::TimedOut
                });
            }
            let wait = (deadline - now).min(RECHECK);
            st = shared
                .freed
                .wait_timeout(st, wait)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    const GIB: u64 = 1 << 30;
    const MIB: u64 = 1 << 20;

    /// A budget over a host whose resident memory the test moves.
    fn budget(limit: u64, rss: &Arc<AtomicU64>, max_workers: usize) -> MemoryBudget {
        let rss = rss.clone();
        MemoryBudget::new(
            Policy::default(),
            max_workers,
            Arc::new(move || HostLimits {
                memory_limit: limit,
                node_rss: rss.load(Ordering::SeqCst),
                cores: None,
                limit_unreadable: false,
            }),
        )
    }

    const SHORT: Duration = Duration::from_millis(50);

    #[test]
    fn a_lone_worker_gets_all_the_headroom_up_to_the_ceiling() {
        let rss = Arc::new(AtomicU64::new(600 * MIB));
        let b = budget(2 * GIB, &rss, 2);
        let r = b.acquire_worker(SHORT).expect("granted");
        assert_eq!(r.bytes(), Policy::default().data_ceiling);
        assert_eq!(b.reserved(), r.bytes());
        drop(r);
        assert_eq!(b.reserved(), 0);
    }

    /// The case the first design got wrong: two slots, sized at start for a small
    /// node, then the node grows. Each worker used to be sized as though it were
    /// alone, so together they passed the service limit.
    #[test]
    fn two_workers_after_the_node_grows_never_pass_the_usable_share() {
        let limit = 8 * GIB;
        let usable = caps::usable(limit);
        let rss = Arc::new(AtomicU64::new(GIB));
        let b = budget(limit, &rss, 2);
        // The node grows to 6.1 GiB before either worker starts.
        rss.store(6 * GIB + GIB / 10, Ordering::SeqCst);
        let first = b.acquire_worker(SHORT).expect("the first fits");
        let second = b.acquire_worker(SHORT);
        assert!(
            matches!(second, Err(Denied::TooSmall { .. })),
            "no room for a second: {:?}",
            second.err()
        );
        assert!(rss.load(Ordering::SeqCst) + b.reserved() <= usable);
        // A worker that is already running is not shrunk when the node grows.
        rss.store(7 * GIB, Ordering::SeqCst);
        assert_eq!(first.bytes(), Policy::default().data_ceiling);
        // Nothing new fits while it runs, and nothing is promised once it ends
        // either: the node alone leaves less than the floor.
        assert!(
            matches!(b.acquire_worker(SHORT), Err(Denied::TooSmall { .. })),
            "waits for the running one, then reports the memory"
        );
        drop(first);
        assert!(matches!(
            b.acquire_worker(SHORT),
            Err(Denied::TooSmall { .. })
        ));
    }

    #[test]
    fn concurrent_reservations_never_exceed_the_usable_share() {
        let limit = 4 * GIB;
        let usable = caps::usable(limit);
        let rss = Arc::new(AtomicU64::new(GIB));
        let b = budget(limit, &rss, 2);
        let worst = Arc::new(AtomicU64::new(0));
        let granted = Arc::new(AtomicU64::new(0));
        let mut threads = Vec::new();
        for t in 0..8u64 {
            let (b, rss, worst, granted) = (b.clone(), rss.clone(), worst.clone(), granted.clone());
            threads.push(std::thread::spawn(move || {
                for i in 0..40u64 {
                    // The node's memory moves between 0.5 and 2 GiB.
                    rss.store(
                        512 * MIB + ((t * 40 + i) * 97 % 16) * 96 * MIB,
                        Ordering::SeqCst,
                    );
                    let got = if i % 2 == 0 {
                        b.acquire_worker(Duration::from_millis(20))
                    } else {
                        b.acquire_in_process(700 * MIB, Duration::from_millis(20))
                    };
                    if let Ok(r) = got {
                        granted.fetch_add(1, Ordering::SeqCst);
                        // What is reserved at this instant, with the node's memory
                        // as it was when this grant was made, fits; the node may
                        // since have grown, which only new grants account for.
                        worst.fetch_max(b.reserved(), Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(2));
                        drop(r);
                    }
                }
            }));
        }
        for t in threads {
            t.join().unwrap();
        }
        assert!(granted.load(Ordering::SeqCst) > 0, "something was granted");
        // The node never passed 2 GiB here, so reservations alone never passed
        // the share left by that: 3.6 - 0.5 GiB.
        assert!(
            worst.load(Ordering::SeqCst) <= usable - 512 * MIB,
            "{}",
            worst.load(Ordering::SeqCst)
        );
        assert_eq!(b.reserved(), 0, "every reservation was released");
    }

    #[test]
    fn a_waiter_gets_the_memory_when_a_worker_ends() {
        let rss = Arc::new(AtomicU64::new(GIB));
        let b = budget(2 * GIB, &rss, 2);
        // Headroom 0.8 GiB: one worker fits at the headroom.
        let first = b.acquire_worker(SHORT).expect("granted");
        let b2 = b.clone();
        let waiter = std::thread::spawn(move || b2.acquire_worker(Duration::from_secs(10)));
        std::thread::sleep(Duration::from_millis(100));
        drop(first);
        let got = waiter
            .join()
            .unwrap()
            .expect("granted after the first ended");
        assert!(got.bytes() >= Policy::default().data_floor);
    }

    #[test]
    fn at_most_max_workers_hold_a_reservation() {
        let rss = Arc::new(AtomicU64::new(0));
        let b = budget(64 * GIB, &rss, 2);
        let a = b.acquire_worker(SHORT).unwrap();
        let c = b.acquire_worker(SHORT).unwrap();
        assert_eq!(b.acquire_worker(SHORT).err(), Some(Denied::TimedOut));
        // In-process reservations are not counted as workers.
        assert!(b.acquire_in_process(GIB, SHORT).is_ok());
        drop((a, c));
    }
}
