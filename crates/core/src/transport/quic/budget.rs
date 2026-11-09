//! The node-wide QUIC buffer budget and each data path's flow-control windows
//! (#5940, buffer-budget design sections 3, 4 and 7).
//!
//! What a data path's peer can make this node hold, and what this node holds
//! to send, scales with two windows set per connection at runtime: the
//! receive window W (quinn's connection-level credit) and the send window S.
//! Each data path has a [`ConnMeter`], registered with the node's
//! [`QuicBudget`], which charges the connection
//!
//! ```text
//! (W + debt) + S_charged x 9/8 + gaps x GAP_BYTES + CONTROL_BUFFER_BYTES
//! ```
//!
//! - **W + debt.** quinn lowers a receive window by recording a shrink debt,
//!   paid by later credits (quinn-proto 0.11.19 `streams/state.rs`,
//!   `set_receive_window` and `add_read_credits`), so the credit the peer may
//!   hold is W plus the debt. The meter mirrors the debt from the bytes the
//!   reader reads, which are the credits quinn returns. Credits from a STOP
//!   also pay it and are not seen here, so the mirror over-estimates, the safe
//!   direction.
//! - **S_charged.** quinn keeps data already buffered above a lowered send
//!   window until it is acknowledged, so after a shrink the previous window
//!   stays charged until every stream opened before the shrink has ended (an
//!   epoch the stream holds, [`ConnMeter::send_epoch`]). The 1/8 is quinn's
//!   sent-packet records, about 150 B per 1200-byte packet in flight (derived).
//! - **Gaps.** Received ranges past the first, counted by the reader
//!   (`path::inbound::ConnRanges`), at [`GAP_BYTES`] each. Their cap `R_conn`
//!   follows the window ([`gap_cap`]).
//! - **Control and short buffers**, a constant.
//!
//! A path at the floor windows is charged the floor grant ([`floor_grant`]).
//! The budget does not refuse paths: how many fit is the aggregate step's cap
//! (PR 5). It decides only growth and shrinking:
//!
//! - A connection grows when it is window-limited on CONTIGUOUS delivery: the
//!   in-order prefix of its streams advanced, over the last interval, at least
//!   [`LIMITED_PERCENT`] of a window per round trip. Bytes read above a gap do
//!   not count, so a peer cannot grow its window by sending gapped data.
//! - A grow at most doubles a window, up to [`MAX_WINDOW_BYTES`], and is
//!   granted only while the connection's charge stays within its share `C`
//!   and the node's total within the budget. `C` is the floor grant plus the
//!   budget above every path's floor grant, split over the connections that
//!   are growing (max-min with a floor).
//! - When a grow does not fit the budget, the connections furthest above their
//!   floor grant are shrunk toward `C`. A receive shrink frees nothing until its
//!   debt is read, so the room comes as their peers' data is read, never by
//!   taking credit back.
//!
//! What is not metered here: unread chunks between two polls of a reader
//! (bounded by arrivals during one read latency, the design's one exception),
//! and the fixed per-connection terms (`config::QUIC_CONNECTION_CHARGE_BYTES`).
//! Shadow paths carry no data and have no meter: they keep the endpoint's
//! initial (floor) windows.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::Mutex;

use super::path::inbound::ConnRanges;
use crate::transport::metrics::QuicDataCounters;

/// The largest receive and send window a connection grows to.
pub(crate) const MAX_WINDOW_BYTES: u64 = 8 * 1024 * 1024;

/// The floor window on a host with at least [`SMALL_HOST_MEMORY`].
pub(crate) const MIN_WINDOW_BYTES: u64 = 128 * 1024;

/// The floor window below [`SMALL_HOST_MEMORY`].
pub(crate) const MIN_WINDOW_SMALL_HOST_BYTES: u64 = 64 * 1024;

/// Hosts with less memory get the smaller floor window.
pub(crate) const SMALL_HOST_MEMORY: usize = 2 * 1024 * 1024 * 1024;

/// What one gap costs: quinn's `recvd` entry and the reader's mirror (derived).
pub(crate) const GAP_BYTES: u64 = 70;

/// The fewest gaps a connection may hold, whatever its window.
pub(crate) const MIN_GAPS_PER_CONNECTION: usize = 256;

/// The control channel and the reader's short buffers, per data path.
pub(crate) const CONTROL_BUFFER_BYTES: u64 = 64 * 1024;

/// A connection is window-limited when its contiguous delivery reached this
/// share of a window per round trip.
pub(crate) const LIMITED_PERCENT: u64 = 75;

/// How long a connection counts as growing after it was last seen
/// window-limited, for the share `C`.
pub(crate) const GROWING_FOR: Duration = Duration::from_secs(1);

/// The shortest interval a meter evaluates over, whatever the round trip.
pub(crate) const MIN_EVALUATION_INTERVAL: Duration = Duration::from_millis(10);

/// The floor window for a node with `memory_limit` bytes.
pub(crate) fn floor_window_bytes(memory_limit: usize) -> u64 {
    if memory_limit < SMALL_HOST_MEMORY {
        MIN_WINDOW_SMALL_HOST_BYTES
    } else {
        MIN_WINDOW_BYTES
    }
}

/// The gap cap `R_conn` for a connection whose peer may hold `credit` bytes
/// (W + debt): one gap per KiB, at least [`MIN_GAPS_PER_CONNECTION`].
pub(crate) fn gap_cap(credit: u64) -> usize {
    usize::try_from(credit / 1024)
        .unwrap_or(usize::MAX)
        .max(MIN_GAPS_PER_CONNECTION)
}

/// The charge of a path at the floor windows with gaps at the floor cap.
pub(crate) fn floor_grant(floor: u64) -> u64 {
    floor + send_charge(floor) + MIN_GAPS_PER_CONNECTION as u64 * GAP_BYTES + CONTROL_BUFFER_BYTES
}

/// A send window's charge with quinn's sent-packet records.
fn send_charge(window: u64) -> u64 {
    window.saturating_add(window / 8)
}

/// What a meter changes on its connection. A trait so the budget's decisions
/// are tested without quinn.
pub(crate) trait WindowControl: Send + Sync {
    fn set_receive_window(&self, bytes: u64);
    fn set_send_window(&self, bytes: u64);
    /// The smoothed round trip.
    fn rtt(&self) -> Duration;
    /// The congestion window.
    fn congestion_window(&self) -> u64;
}

impl WindowControl for quinn::Connection {
    fn set_receive_window(&self, bytes: u64) {
        // A window is at most MAX_WINDOW_BYTES, far inside a VarInt.
        if let Ok(window) = quinn::VarInt::from_u64(bytes) {
            quinn::Connection::set_receive_window(self, window);
        }
    }

    fn set_send_window(&self, bytes: u64) {
        quinn::Connection::set_send_window(self, bytes);
    }

    fn rtt(&self) -> Duration {
        quinn::Connection::rtt(self)
    }

    fn congestion_window(&self) -> u64 {
        self.congestion_state().window()
    }
}

/// The node's budget for QUIC memory that scales with the windows.
pub(crate) struct QuicBudget {
    total: u64,
    floor: u64,
    node: &'static QuicDataCounters,
    state: Mutex<BudgetState>,
}

#[derive(Default)]
struct BudgetState {
    conns: HashMap<u64, ConnState>,
    next_id: u64,
}

struct ConnState {
    control: Arc<dyn WindowControl>,
    ranges: Arc<ConnRanges>,
    recv_window: u64,
    /// The mirror of quinn's shrink debt.
    debt: u64,
    send_window: u64,
    /// The epoch streams opened now hold.
    send_epoch: Arc<()>,
    /// Windows from before a send shrink, charged while a stream of their epoch
    /// is open.
    held_send: Vec<(u64, Weak<()>)>,
    /// Until when the connection counts as growing, in nanoseconds.
    growing_until: u64,
}

impl ConnState {
    /// The send window charged with `send` as the current one: the largest of
    /// it and the windows held from before a shrink whose streams are open.
    fn send_charged_with(&mut self, send: u64) -> u64 {
        self.held_send.retain(|(_, epoch)| epoch.strong_count() > 0);
        self.held_send
            .iter()
            .map(|(window, _)| *window)
            .fold(send, u64::max)
    }

    /// The terms no window sets: gaps and the control buffers.
    fn fixed(&self) -> u64 {
        (self.ranges.held_now() as u64)
            .saturating_mul(GAP_BYTES)
            .saturating_add(CONTROL_BUFFER_BYTES)
    }

    fn charge_with(&mut self, recv_window: u64, send_window: u64) -> u64 {
        let send = self.send_charged_with(send_window);
        recv_window
            .saturating_add(self.debt)
            .saturating_add(send_charge(send))
            .saturating_add(self.fixed())
    }

    fn charge(&mut self) -> u64 {
        self.charge_with(self.recv_window, self.send_window)
    }

    fn set_recv(&mut self, window: u64) {
        if window < self.recv_window {
            self.debt = self.debt.saturating_add(self.recv_window - window);
        }
        self.recv_window = window;
        self.control.set_receive_window(window);
        self.ranges
            .set_cap(gap_cap(self.recv_window.saturating_add(self.debt)));
    }

    fn set_send(&mut self, window: u64) {
        if window < self.send_window {
            let old = std::mem::replace(&mut self.send_epoch, Arc::new(()));
            self.held_send
                .push((self.send_window, Arc::downgrade(&old)));
        }
        self.send_window = window;
        self.control.set_send_window(window);
    }
}

/// What one evaluation saw, for tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Evaluation {
    pub(crate) limited: bool,
    pub(crate) recv_window: u64,
    pub(crate) send_window: u64,
}

impl QuicBudget {
    /// A budget of `total` bytes, for paths whose windows start at `floor`.
    pub(crate) fn new(total: u64, floor: u64, node: &'static QuicDataCounters) -> Arc<Self> {
        node.set_budget_bytes(total);
        Arc::new(Self {
            total,
            floor,
            node,
            state: Mutex::new(BudgetState::default()),
        })
    }

    #[cfg(test)]
    pub(crate) fn floor(&self) -> u64 {
        self.floor
    }

    pub(crate) fn total(&self) -> u64 {
        self.total
    }

    /// Register a data path's connection at the floor windows. Its gap cap
    /// follows its window from now on.
    pub(in crate::transport::quic) fn register(
        self: &Arc<Self>,
        control: Arc<dyn WindowControl>,
        ranges: Arc<ConnRanges>,
        now: u64,
    ) -> ConnMeter {
        let mut state = self.state.lock();
        let id = state.next_id;
        state.next_id += 1;
        ranges.set_cap(gap_cap(self.floor));
        let send_epoch = Arc::new(());
        state.conns.insert(
            id,
            ConnState {
                control,
                ranges,
                recv_window: self.floor,
                debt: 0,
                send_window: self.floor,
                send_epoch,
                held_send: Vec::new(),
                growing_until: 0,
            },
        );
        self.publish(&mut state);
        ConnMeter {
            id,
            budget: self.clone(),
            read: AtomicU64::new(0),
            delivered: AtomicU64::new(0),
            evaluated_at: AtomicU64::new(now),
        }
    }

    /// The sum of every connection's charge.
    #[cfg(test)]
    pub(crate) fn charged(&self) -> u64 {
        let mut state = self.state.lock();
        Self::sum(&mut state)
    }

    fn sum(state: &mut BudgetState) -> u64 {
        state.conns.values_mut().map(ConnState::charge).sum()
    }

    fn publish(&self, state: &mut BudgetState) {
        self.node.set_budget_charged(Self::sum(state));
    }

    /// The share cap `C` at `now`: the floor grant plus the budget above every
    /// path's floor grant, split over the connections growing.
    fn share(&self, state: &BudgetState, now: u64) -> u64 {
        let grant = floor_grant(self.floor);
        let floors = grant.saturating_mul(state.conns.len() as u64);
        let growing = state
            .conns
            .values()
            .filter(|conn| conn.growing_until > now)
            .count()
            .max(1) as u64;
        grant.saturating_add(self.total.saturating_sub(floors) / growing)
    }

    fn evaluate(
        &self,
        id: u64,
        read: u64,
        delivered: u64,
        elapsed: u64,
        now: u64,
    ) -> Option<Evaluation> {
        let mut state = self.state.lock();
        let (limited, cwnd) = {
            let conn = state.conns.get_mut(&id)?;
            conn.debt = conn.debt.saturating_sub(read);
            let rtt = conn.control.rtt().max(Duration::from_millis(1));
            let rtt_nanos = u64::try_from(rtt.as_nanos()).unwrap_or(u64::MAX);
            // Contiguous delivery per round trip reached LIMITED_PERCENT of the
            // window.
            let limited = elapsed > 0
                && u128::from(delivered) * u128::from(rtt_nanos) * 100
                    >= u128::from(conn.recv_window)
                        * u128::from(elapsed)
                        * u128::from(LIMITED_PERCENT);
            if limited {
                conn.growing_until = now.saturating_add(nanos(GROWING_FOR));
            }
            (limited, conn.control.congestion_window())
        };
        let share = self.share(&state, now);
        let others = Self::sum_except(&mut state, id);
        let allowed = share.min(self.total.saturating_sub(others));
        let mut squeeze_others = false;
        let conn = state.conns.get_mut(&id)?;
        if limited && conn.recv_window < MAX_WINDOW_BYTES {
            let target = conn.recv_window.saturating_mul(2).min(MAX_WINDOW_BYTES);
            let send = conn.send_window;
            let window = largest_fitting(conn.recv_window, target, |w| {
                conn.charge_with(w, send) <= allowed
            });
            if window > conn.recv_window {
                conn.set_recv(window);
                self.node.record_window_change(WindowChange::Grew);
            } else {
                self.node.record_window_change(WindowChange::Refused);
            }
            // Held back by the budget rather than by its share: others are
            // above theirs. They make room as their debt is read.
            squeeze_others = window < target && allowed < share;
        } else if others.saturating_add(conn.charge()) > self.total
            && shrink_toward(conn, share, self.floor)
        {
            self.node.record_window_change(WindowChange::Shrank);
        }
        // The send window follows the congestion window, within what is
        // allowed; it is lowered only once it is twice what is needed, so a
        // congestion window that moves does not open an epoch per evaluation.
        let send_target = cwnd.saturating_mul(2).clamp(self.floor, MAX_WINDOW_BYTES);
        if send_target > conn.send_window {
            let recv = conn.recv_window;
            let window = largest_fitting(conn.send_window, send_target, |w| {
                conn.charge_with(recv, w) <= allowed
            });
            if window > conn.send_window {
                conn.set_send(window);
            }
        } else if send_target.saturating_mul(2) <= conn.send_window {
            conn.set_send(send_target);
        }
        let result = Evaluation {
            limited,
            recv_window: conn.recv_window,
            send_window: conn.send_window,
        };
        if squeeze_others {
            self.shrink_others(&mut state, id, share);
        }
        self.publish(&mut state);
        Some(result)
    }

    fn sum_except(state: &mut BudgetState, id: u64) -> u64 {
        state
            .conns
            .iter_mut()
            .filter(|(other, _)| **other != id)
            .map(|(_, conn)| conn.charge())
            .sum()
    }

    /// Shrink the connections furthest above their floor grant toward `share`,
    /// until the node's charge after their debts are read fits the budget.
    fn shrink_others(&self, state: &mut BudgetState, id: u64, share: u64) {
        let mut over: Vec<(u64, u64)> = state
            .conns
            .iter_mut()
            .filter(|(other, _)| **other != id)
            .map(|(other, conn)| (*other, conn.charge()))
            .filter(|(_, charge)| *charge > share)
            .collect();
        over.sort_unstable_by_key(|(_, charge)| std::cmp::Reverse(*charge));
        for (other, _) in over {
            if let Some(conn) = state.conns.get_mut(&other)
                && shrink_toward(conn, share, self.floor)
            {
                self.node.record_window_change(WindowChange::Shrank);
            }
        }
    }

    fn deregister(&self, id: u64) {
        let mut state = self.state.lock();
        state.conns.remove(&id);
        self.publish(&mut state);
    }

    fn send_epoch(&self, id: u64) -> Option<Arc<()>> {
        self.state
            .lock()
            .conns
            .get(&id)
            .map(|conn| conn.send_epoch.clone())
    }

    #[cfg(test)]
    fn conn_charge(&self, id: u64) -> Option<u64> {
        self.state.lock().conns.get_mut(&id).map(ConnState::charge)
    }

    #[cfg(test)]
    fn debt(&self, id: u64) -> Option<u64> {
        self.state.lock().conns.get(&id).map(|conn| conn.debt)
    }
}

/// Lower `conn`'s windows until its charge, once its debt is read, is at most
/// `share`, not below `floor`: the receive window first, then the send window.
/// Whether anything was lowered.
fn shrink_toward(conn: &mut ConnState, share: u64, floor: u64) -> bool {
    let mut lowered = false;
    let room = share.saturating_sub(conn.fixed());
    let recv = room
        .saturating_sub(send_charge(conn.send_window))
        .max(floor);
    if recv < conn.recv_window {
        conn.set_recv(recv);
        lowered = true;
    }
    let send = (room.saturating_sub(conn.recv_window).saturating_mul(8) / 9).max(floor);
    if send < conn.send_window {
        conn.set_send(send);
        lowered = true;
    }
    lowered
}

/// The largest window in `from..=to` that `fits`, at 4 KiB steps; `from` if none
/// larger does. `fits` is monotone (a larger window never fits where a smaller
/// does not).
fn largest_fitting(from: u64, to: u64, mut fits: impl FnMut(u64) -> bool) -> u64 {
    if to <= from || !fits(from.saturating_add(4096).min(to)) {
        return from;
    }
    let (mut lo, mut hi) = (from, to);
    if fits(hi) {
        return hi;
    }
    while hi - lo > 4096 {
        let mid = lo + (hi - lo) / 2;
        if fits(mid) {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    lo
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// What an evaluation did to a window, counted node-wide.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WindowChange {
    Grew,
    /// Window-limited, but no room to grow.
    Refused,
    Shrank,
}

/// One data path's share of the budget. Dropping it frees the share.
pub(crate) struct ConnMeter {
    id: u64,
    budget: Arc<QuicBudget>,
    /// Bytes read since the last evaluation: quinn's credits.
    read: AtomicU64,
    /// Contiguous bytes delivered since the last evaluation.
    delivered: AtomicU64,
    evaluated_at: AtomicU64,
}

impl ConnMeter {
    /// The reader read `bytes`, of which `contiguous` extended a stream's
    /// in-order prefix, at `now`. Evaluates at most once per round trip (at
    /// least [`MIN_EVALUATION_INTERVAL`] apart).
    pub(crate) fn on_read(&self, bytes: u64, contiguous: u64, now: u64) {
        self.read.fetch_add(bytes, Ordering::Relaxed);
        self.delivered.fetch_add(contiguous, Ordering::Relaxed);
        self.maybe_evaluate(now);
    }

    /// Evaluate if an interval has passed: from the reader, and from the path's
    /// periodic tick so an idle connection's send window and held epochs are
    /// settled too.
    pub(crate) fn maybe_evaluate(&self, now: u64) -> Option<Evaluation> {
        let last = self.evaluated_at.load(Ordering::Relaxed);
        let interval = nanos(MIN_EVALUATION_INTERVAL);
        if now.saturating_sub(last) < interval {
            return None;
        }
        self.evaluate(now)
    }

    /// Evaluate now, whatever the interval.
    pub(crate) fn evaluate(&self, now: u64) -> Option<Evaluation> {
        let last = self.evaluated_at.swap(now, Ordering::Relaxed);
        let read = self.read.swap(0, Ordering::Relaxed);
        let delivered = self.delivered.swap(0, Ordering::Relaxed);
        self.budget
            .evaluate(self.id, read, delivered, now.saturating_sub(last), now)
    }

    /// The epoch a stream opened now holds until it ends, which keeps a
    /// shrunk send window charged while quinn may still hold its data.
    pub(crate) fn send_epoch(&self) -> Option<Arc<()>> {
        self.budget.send_epoch(self.id)
    }

    #[cfg(test)]
    pub(crate) fn charge(&self) -> u64 {
        self.budget.conn_charge(self.id).unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn debt(&self) -> u64 {
        self.budget.debt(self.id).unwrap_or(0)
    }
}

impl Drop for ConnMeter {
    fn drop(&mut self) {
        self.budget.deregister(self.id);
    }
}

#[cfg(test)]
mod tests;
