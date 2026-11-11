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
//! receive_charge(W + debt) + S_charged x 9/8 + R_conn x GAP_BYTES + CONTROL_BUFFER_BYTES
//! ```
//!
//! - **receive_charge(W + debt)**, 3 x (W + debt): the bytes the peer may have
//!   sent that the reader has not read yet (at most the credit), plus what the
//!   reader may stage for streams whose header it has not taken yet
//!   (`path::inbound::ConnStaging`, capped at [`staging_cap`], twice the
//!   credit). Every stream is read unordered from its first byte, so quinn
//!   holds nothing for a stream beyond the chunks its reader has not polled.
//! - **W + debt.** quinn lowers a receive window by recording a shrink debt,
//!   paid by later credits (quinn-proto 0.11.19 `streams/state.rs`,
//!   `set_receive_window` and `add_read_credits`), so the credit the peer may
//!   hold is W plus the debt. The meter mirrors the debt from the bytes the
//!   reader reads, which are the credits quinn returns. Every window change
//!   and every read of a connection happens in that connection's receive task
//!   (a shrink another connection asks for is applied there,
//!   [`ConnState::shrink_to`]), so the mirror sees each read before or after
//!   each change exactly as quinn did. Credits from a STOP also pay the debt
//!   and are not seen here, so the mirror over-estimates, the safe direction.
//! - **S_charged.** quinn keeps data already buffered above a lowered send
//!   window until it is acknowledged, so after a shrink the previous window
//!   stays charged until every stream opened before the shrink has ended (an
//!   epoch the stream holds, [`ConnMeter::send_epoch`]). The 1/8 is quinn's
//!   sent-packet records, about 150 B per 1200-byte packet in flight (derived).
//! - **Gaps.** Received ranges past the first, counted by the reader
//!   (`path::inbound::ConnRanges`), at [`GAP_BYTES`] each, charged at their
//!   cap `R_conn` ([`gap_cap`]), so the charge does not move as gaps come and
//!   go. The cap follows W + debt and is never set below the gaps held.
//! - **Control and short buffers**, a constant.
//!
//! A path at the floor windows is charged the floor grant ([`floor_grant`]).
//! The budget does not refuse paths: how many fit is the aggregate step's cap
//! (PR 5). It decides only growth and shrinking:
//!
//! - A connection grows when it is window-limited on CONTIGUOUS delivery: the
//!   in-order prefix of its streams advanced, over the last interval (at least
//!   one round trip, [`MIN_EVALUATION_INTERVAL`] at least), at least
//!   [`LIMITED_PERCENT`] of a window per round trip. Bytes read above a gap do
//!   not count, so a peer cannot grow its window by sending gapped data.
//! - A grow at most doubles a window, up to [`MAX_WINDOW_BYTES`], and is
//!   granted only while the connection's charge stays within its share `C`
//!   and the node's total within the budget. `C` is the floor grant plus the
//!   budget above every path's floor grant, split over the connections that
//!   are growing (max-min with a floor).
//! - When a grow does not fit the budget, the connections furthest above their
//!   floor grant are asked to shrink toward `C`. A receive shrink frees nothing
//!   until its debt is read, so the room comes as their peers' data is read,
//!   never by taking credit back.
//! - A connection that has delivered less than a quarter of its window per
//!   round trip for [`IDLE_DECAY`] halves
//!   its receive window toward the floor, once per [`IDLE_DECAY`]. That stops
//!   new credit; credit already granted stays charged until the peer uses it
//!   (QUIC cannot take it back), so a peer that grew and then went silent
//!   keeps its charge for as long as it stays silent. That limits other paths'
//!   growth, not memory.
//!
//! Each evaluation updates a running total under one lock and does no work
//! proportional to the number of paths, except when a connection grows (it
//! counts the paths growing) or squeezes others; quinn is told of a window
//! change after the lock is released.
//!
//! What is not metered here: unread chunks between two polls of a reader
//! (bounded by arrivals during one read latency, the design's one exception),
//! and the fixed per-connection terms (`config::QUIC_CONNECTION_CHARGE_BYTES`).
//! Shadow paths carry no data and have no meter: they keep the endpoint's
//! initial windows.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::Mutex;

use super::path::inbound::{ConnRanges, ConnStaging};
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

/// A connection's staged bytes may reach this many times its credit
/// (provisional: the hostile-sender harness measures honest stops under loss,
/// and the ramp gate needs them near zero at 5%).
pub(crate) const STAGING_CREDIT_MULTIPLE: u64 = 2;

/// A connection is window-limited when its contiguous delivery reached this
/// share of a window per round trip.
pub(crate) const LIMITED_PERCENT: u64 = 75;

/// How long a connection counts as growing after it was last seen
/// window-limited, for the share `C`.
pub(crate) const GROWING_FOR: Duration = Duration::from_secs(1);

/// The shortest interval a meter evaluates over, whatever the round trip.
pub(crate) const MIN_EVALUATION_INTERVAL: Duration = Duration::from_millis(10);

/// How long a connection must go without being window-limited before its
/// receive window is halved toward the floor, and how often it is halved
/// again.
pub(crate) const IDLE_DECAY: Duration = Duration::from_secs(10);

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

/// The cap of a connection's staged bytes when its peer may hold `credit`
/// bytes (W + debt).
pub(crate) fn staging_cap(credit: u64) -> usize {
    usize::try_from(credit.saturating_mul(STAGING_CREDIT_MULTIPLE)).unwrap_or(usize::MAX)
}

/// What the receive side may hold for a connection whose peer may hold
/// `credit` bytes: the unread credit plus the staging cap.
pub(crate) const fn receive_charge(credit: u64) -> u64 {
    credit.saturating_mul(1 + STAGING_CREDIT_MULTIPLE)
}

/// The largest credit whose [`receive_charge`] is at most `charge`.
const fn credit_for_receive_charge(charge: u64) -> u64 {
    charge / (1 + STAGING_CREDIT_MULTIPLE)
}

/// The charge of a path at the floor windows with gaps at the floor cap.
pub(crate) fn floor_grant(floor: u64) -> u64 {
    receive_charge(floor)
        + send_charge(floor)
        + gap_cap(floor) as u64 * GAP_BYTES
        + CONTROL_BUFFER_BYTES
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
    /// The sum of every connection's `charged`.
    charged: u64,
    /// The share cap and when it was computed: recomputed at most once per
    /// [`MIN_EVALUATION_INTERVAL`] node-wide, since counting the connections
    /// growing takes a pass over all of them.
    share: Option<(u64, u64)>,
}

struct ConnState {
    ranges: Arc<ConnRanges>,
    staging: Arc<ConnStaging>,
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
    /// When the connection last delivered a quarter of its window per round
    /// trip, or its receive window was last halved for idleness
    /// ([`IDLE_DECAY`]).
    active_at: u64,
    /// The charge this connection adds to the node's total, as of its last
    /// change. Epochs that end lower its real charge until the next
    /// recount, so this over-estimates in between, the safe direction.
    charged: u64,
    /// A shrink another connection asked for, applied in this connection's
    /// own evaluation so that quinn sees every window change of a connection
    /// in the order the meter's debt mirror assumes (receive window, then
    /// send window).
    shrink_to: Option<(u64, u64)>,
}

/// Window changes an evaluation decided, made on the connection once the
/// budget's lock is released.
#[derive(Default)]
struct Changes {
    recv: Option<u64>,
    send: Option<u64>,
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

    /// The terms no window sets directly: gaps at their cap and the control
    /// buffers.
    fn fixed_with(&self, credit: u64) -> u64 {
        (gap_cap(credit).max(self.ranges.held_now()) as u64)
            .saturating_mul(GAP_BYTES)
            .saturating_add(CONTROL_BUFFER_BYTES)
    }

    fn charge_with(&mut self, recv_window: u64, send_window: u64) -> u64 {
        let credit = recv_window.saturating_add(self.debt);
        let send = self.send_charged_with(send_window);
        receive_charge(credit)
            .saturating_add(send_charge(send))
            .saturating_add(self.fixed_with(credit))
    }

    fn charge(&mut self) -> u64 {
        self.charge_with(self.recv_window, self.send_window)
    }

    fn set_recv(&mut self, window: u64, changes: &mut Changes) {
        if window < self.recv_window {
            self.debt = self.debt.saturating_add(self.recv_window - window);
        }
        self.recv_window = window;
        changes.recv = Some(window);
        self.credit_changed();
    }

    /// W + debt moved: the gap cap and the staging cap follow it. The gap cap
    /// is never set below the gaps already held, so a shrink cannot make the
    /// next honest gap a trip.
    fn credit_changed(&self) {
        let credit = self.recv_window.saturating_add(self.debt);
        self.ranges
            .set_cap(gap_cap(credit).max(self.ranges.held_now()));
        self.staging.set_cap(staging_cap(credit));
    }

    fn set_send(&mut self, window: u64, changes: &mut Changes) {
        if window < self.send_window {
            let old = std::mem::replace(&mut self.send_epoch, Arc::new(()));
            self.held_send
                .push((self.send_window, Arc::downgrade(&old)));
        }
        self.send_window = window;
        changes.send = Some(window);
    }
}

/// What one evaluation saw, for tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Evaluation {
    pub(crate) limited: bool,
    /// The budget held the connection below what it needed: a grow it
    /// needed was refused or cut short, or another path's growth shrank it.
    pub(crate) throttled: bool,
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

    pub(crate) fn total(&self) -> u64 {
        self.total
    }

    /// Register a data path's connection at the floor windows, which are set
    /// on it. The connection must not already have a larger receive window:
    /// lowering it here would leave quinn a debt the meter does not mirror
    /// (the endpoint starts every connection below the floor,
    /// `config::INITIAL_RECEIVE_WINDOW_BYTES`). Its gap cap and staging cap
    /// follow its credit from now on.
    pub(in crate::transport::quic) fn register(
        self: &Arc<Self>,
        control: Arc<dyn WindowControl>,
        ranges: Arc<ConnRanges>,
        staging: Arc<ConnStaging>,
        now: u64,
    ) -> ConnMeter {
        let id = {
            let mut state = self.state.lock();
            let id = state.next_id;
            state.next_id += 1;
            let mut conn = ConnState {
                ranges,
                staging,
                recv_window: self.floor,
                debt: 0,
                send_window: self.floor,
                send_epoch: Arc::new(()),
                held_send: Vec::new(),
                growing_until: 0,
                active_at: now,
                charged: 0,
                shrink_to: None,
            };
            conn.credit_changed();
            conn.charged = conn.charge();
            state.charged = state.charged.saturating_add(conn.charged);
            state.conns.insert(id, conn);
            self.publish(&state);
            id
        };
        control.set_receive_window(self.floor);
        control.set_send_window(self.floor);
        ConnMeter {
            id,
            budget: self.clone(),
            control,
            read: AtomicU64::new(0),
            delivered: AtomicU64::new(0),
            evaluated_at: AtomicU64::new(now),
            interval: AtomicU64::new(nanos(MIN_EVALUATION_INTERVAL)),
            wake: tokio::sync::Notify::new(),
            throttled_until: AtomicU64::new(0),
            throttle_began: AtomicU64::new(0),
        }
    }

    /// The sum of every connection's charge, recounted.
    #[cfg(test)]
    pub(crate) fn charged(&self) -> u64 {
        let mut state = self.state.lock();
        state.conns.values_mut().map(ConnState::charge).sum()
    }

    /// The running total. Tests only.
    #[cfg(test)]
    pub(crate) fn running_total(&self) -> u64 {
        self.state.lock().charged
    }

    fn publish(&self, state: &BudgetState) {
        self.node.set_budget_charged(state.charged);
    }

    /// Set `conn`'s charge from a recount, keeping the running total.
    fn recount(total: &mut u64, conn: &mut ConnState) {
        let charge = conn.charge();
        *total = total.saturating_sub(conn.charged).saturating_add(charge);
        conn.charged = charge;
    }

    /// The share cap `C` at `now`: the floor grant plus the budget above every
    /// path's floor grant, split over the connections growing. Cached for
    /// [`MIN_EVALUATION_INTERVAL`]; a connection that starts growing in that
    /// time is counted at the next recount, so for that long a share can be
    /// up to one connection's worth too large, which the node's budget still
    /// bounds.
    fn share(&self, state: &mut BudgetState, now: u64) -> u64 {
        if let Some((at, share)) = state.share
            && now.saturating_sub(at) < nanos(MIN_EVALUATION_INTERVAL)
        {
            return share;
        }
        let share = Self::count_share(self.floor, self.total, state, now);
        state.share = Some((now, share));
        share
    }

    fn count_share(floor: u64, total: u64, state: &BudgetState, now: u64) -> u64 {
        let grant = floor_grant(floor);
        let floors = grant.saturating_mul(state.conns.len() as u64);
        let growing = state
            .conns
            .values()
            .filter(|conn| conn.growing_until > now)
            .count()
            .max(1) as u64;
        grant.saturating_add(total.saturating_sub(floors) / growing)
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate(
        &self,
        id: u64,
        read: u64,
        delivered: u64,
        elapsed: u64,
        rtt: Duration,
        cwnd: u64,
        now: u64,
    ) -> Option<(Evaluation, Changes)> {
        let mut guard = self.state.lock();
        let state = &mut *guard;
        let mut changes = Changes::default();
        let mut throttled = false;
        let limited = {
            let conn = state.conns.get_mut(&id)?;
            // Reads made before any change this evaluation makes, so they paid
            // the debt quinn had then.
            if read > 0 && conn.debt > 0 {
                conn.debt = conn.debt.saturating_sub(read);
                conn.credit_changed();
            }
            if let Some((recv, send)) = conn.shrink_to.take() {
                if recv < conn.recv_window {
                    conn.set_recv(recv, &mut changes);
                    self.node.record_window_change(WindowChange::Shrank);
                    throttled = true;
                }
                if send < conn.send_window {
                    conn.set_send(send, &mut changes);
                }
            }
            let rtt_nanos = nanos(rtt.max(Duration::from_millis(1)));
            // Contiguous delivery per round trip reached LIMITED_PERCENT of the
            // window.
            let limited = elapsed > 0
                && u128::from(delivered) * u128::from(rtt_nanos) * 100
                    >= u128::from(conn.recv_window)
                        * u128::from(elapsed)
                        * u128::from(LIMITED_PERCENT);
            if limited {
                if conn.growing_until <= now {
                    // A new grower changes everyone's share.
                    state.share = None;
                }
                conn.growing_until = now.saturating_add(nanos(GROWING_FOR));
            }
            // Busy: a quarter of a window per round trip. Below that for
            // IDLE_DECAY, the window decays (below).
            if elapsed > 0
                && u128::from(delivered) * u128::from(rtt_nanos) * 4
                    >= u128::from(conn.recv_window) * u128::from(elapsed)
            {
                conn.active_at = now;
            }
            Self::recount(&mut state.charged, conn);
            limited
        };
        let grows = limited && state.conns.get(&id)?.recv_window < MAX_WINDOW_BYTES;
        let share = self.share(state, now);
        let conn = state.conns.get_mut(&id)?;
        let others = state.charged.saturating_sub(conn.charged);
        let allowed = share.min(self.total.saturating_sub(others));
        let mut squeeze_others = false;
        if grows {
            let target = conn.recv_window.saturating_mul(2).min(MAX_WINDOW_BYTES);
            let send = conn.send_window;
            let window = largest_fitting(conn.recv_window, target, |w| {
                conn.charge_with(w, send) <= allowed
            });
            if window > conn.recv_window {
                conn.set_recv(window, &mut changes);
                self.node.record_window_change(WindowChange::Grew);
            } else {
                self.node.record_window_change(WindowChange::Refused);
            }
            // Held back by the budget rather than by its share: others are
            // above theirs. They make room as their debt is read.
            squeeze_others = window < target && allowed < share;
            throttled |= window < target;
        } else if conn.recv_window > self.floor
            && now.saturating_sub(conn.active_at) >= nanos(IDLE_DECAY)
        {
            conn.active_at = now;
            conn.set_recv((conn.recv_window / 2).max(self.floor), &mut changes);
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
                conn.set_send(window, &mut changes);
            }
        } else if send_target.saturating_mul(2) <= conn.send_window {
            conn.set_send(send_target, &mut changes);
        }
        let result = Evaluation {
            limited,
            throttled,
            recv_window: conn.recv_window,
            send_window: conn.send_window,
        };
        Self::recount(&mut state.charged, conn);
        if squeeze_others {
            self.squeeze_others(state, id, share);
        }
        self.publish(state);
        Some((result, changes))
    }

    /// Ask the connections whose charge is above `share` to shrink toward it,
    /// largest first, until the node's charge after their debts are read fits
    /// the budget. Each applies the shrink in its own next evaluation.
    fn squeeze_others(&self, state: &mut BudgetState, id: u64, share: u64) {
        let mut over: Vec<(u64, u64)> = state
            .conns
            .iter()
            .filter(|(other, conn)| **other != id && conn.charged > share)
            .map(|(other, conn)| (*other, conn.charged))
            .collect();
        over.sort_unstable_by_key(|(_, charge)| std::cmp::Reverse(*charge));
        for (other, _) in over {
            if let Some(conn) = state.conns.get_mut(&other) {
                conn.shrink_to = shrink_target(conn, share, self.floor);
            }
        }
    }

    fn deregister(&self, id: u64) {
        let mut state = self.state.lock();
        if let Some(conn) = state.conns.remove(&id) {
            state.charged = state.charged.saturating_sub(conn.charged);
        }
        self.publish(&state);
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

    #[cfg(test)]
    fn pending_shrink(&self, id: u64) -> Option<(u64, u64)> {
        self.state
            .lock()
            .conns
            .get(&id)
            .and_then(|conn| conn.shrink_to)
    }
}

/// The windows that bring `conn`'s charge, once its debt is read, to at most
/// `share`, not below `floor`: the receive window first, then the send window.
/// `None` if neither would be lowered.
fn shrink_target(conn: &mut ConnState, share: u64, floor: u64) -> Option<(u64, u64)> {
    let credit = conn.recv_window.saturating_add(conn.debt);
    let room = share.saturating_sub(conn.fixed_with(credit));
    let recv = credit_for_receive_charge(room.saturating_sub(send_charge(conn.send_window)))
        .max(floor)
        .min(conn.recv_window);
    let send = (room.saturating_sub(receive_charge(recv)).saturating_mul(8) / 9)
        .max(floor)
        .min(conn.send_window);
    (recv < conn.recv_window || send < conn.send_window).then_some((recv, send))
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
///
/// Evaluated only from the path's receive task (its reads, its periodic
/// tick, and a wake when its sends progress, [`Self::note_send`]), so the
/// window changes it makes and the reads it mirrors are never concurrent.
pub(crate) struct ConnMeter {
    id: u64,
    budget: Arc<QuicBudget>,
    control: Arc<dyn WindowControl>,
    /// Bytes read since the last evaluation: quinn's credits.
    read: AtomicU64,
    /// Contiguous bytes delivered since the last evaluation.
    delivered: AtomicU64,
    evaluated_at: AtomicU64,
    /// The interval between evaluations: the last round trip seen, at least
    /// [`MIN_EVALUATION_INTERVAL`].
    interval: AtomicU64,
    /// Woken by sends, so a path that only sends still has its send window
    /// evaluated ([`Self::send_progress`]).
    wake: tokio::sync::Notify,
    /// Until when the connection counts as throttled, in nanoseconds
    /// ([`Self::throttled_since`]).
    throttled_until: AtomicU64,
    /// When the current throttle began.
    throttle_began: AtomicU64,
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

    /// A send on the path made progress: wake the receive task, which
    /// evaluates if an interval has passed ([`Self::send_progress`]). Called
    /// from the send's own task; it evaluates nothing itself.
    pub(crate) fn note_send(&self) {
        self.wake.notify_one();
    }

    /// Completes when a send made progress since the last time it completed.
    pub(crate) async fn send_progress(&self) {
        self.wake.notified().await;
    }

    /// Evaluate if an interval has passed: from the reader, from the path's
    /// periodic tick so an idle connection's windows and held epochs are
    /// settled too, and when its sends progress.
    pub(crate) fn maybe_evaluate(&self, now: u64) -> Option<Evaluation> {
        let last = self.evaluated_at.load(Ordering::Relaxed);
        if now.saturating_sub(last) < self.interval.load(Ordering::Relaxed) {
            return None;
        }
        self.evaluate(now)
    }

    /// Evaluate now, whatever the interval.
    pub(crate) fn evaluate(&self, now: u64) -> Option<Evaluation> {
        let last = self.evaluated_at.swap(now, Ordering::Relaxed);
        let read = self.read.swap(0, Ordering::Relaxed);
        let delivered = self.delivered.swap(0, Ordering::Relaxed);
        let rtt = self.control.rtt();
        self.interval
            .store(nanos(rtt.max(MIN_EVALUATION_INTERVAL)), Ordering::Relaxed);
        let (result, changes) = self.budget.evaluate(
            self.id,
            read,
            delivered,
            now.saturating_sub(last),
            rtt,
            self.control.congestion_window(),
            now,
        )?;
        if result.throttled {
            if self.throttled_until.load(Ordering::Relaxed) <= now {
                self.throttle_began.store(now, Ordering::Relaxed);
            }
            let hold = self
                .interval
                .load(Ordering::Relaxed)
                .max(nanos(GROWING_FOR));
            self.throttled_until
                .store(now.saturating_add(hold), Ordering::Relaxed);
        }
        // After the budget's lock is released, in the order decided.
        if let Some(window) = changes.recv {
            self.control.set_receive_window(window);
        }
        if let Some(window) = changes.send {
            self.control.set_send_window(window);
        }
        Some(result)
    }

    /// When the budget began holding this connection below what it needs,
    /// if it still does at `now`: an evaluation found it throttled
    /// ([`Evaluation::throttled`]) within the last [`GROWING_FOR`] (or round
    /// trip, if longer). Our decision, so its readers do not count the
    /// slowdown it causes as the peer's idleness (buffer-budget design M6).
    pub(crate) fn throttled_since(&self, now: u64) -> Option<u64> {
        (now < self.throttled_until.load(Ordering::Relaxed))
            .then(|| self.throttle_began.load(Ordering::Relaxed))
    }

    /// Count the connection as throttled from `began` until `until`. Tests
    /// only, for the readers' side of a throttle.
    #[cfg(test)]
    pub(crate) fn throttle_for_test(&self, began: u64, until: u64) {
        self.throttle_began.store(began, Ordering::Relaxed);
        self.throttled_until.store(until, Ordering::Relaxed);
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

    #[cfg(test)]
    pub(crate) fn pending_shrink(&self) -> Option<(u64, u64)> {
        self.budget.pending_shrink(self.id)
    }
}

impl Drop for ConnMeter {
    fn drop(&mut self) {
        self.budget.deregister(self.id);
    }
}

#[cfg(test)]
mod tests;
