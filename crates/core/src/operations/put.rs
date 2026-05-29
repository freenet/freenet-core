//! A contract is PUT within a location distance: all nodes within a
//! given radius cache a copy of the contract and its current value,
//! and broadcast updates to subscribers.
//!
//! Every PUT wire variant dispatches to a driver —
//! `op_ctx_task::start_client_put`, `start_relay_put`, and
//! `start_relay_put_streaming`. The wire-format types
//! (`PutMsg`, `PutStreamingPayload`), the originator finalization
//! helpers, and `put_contract` survive here because the drivers
//! consume them.

pub(crate) mod op_ctx_task;

pub(crate) use self::messages::{PutMsg, PutStreamingPayload};
use freenet_stdlib::prelude::*;

use super::OpError;
use crate::{
    contract::ContractHandlerEvent, message::Transaction, node::OpManager, ring::PeerKeyLocation,
    tracing::NetEventLog,
};
use either::Either;

/// Upper bound on the `cause` carried by [`PutMsg::Error`] and
/// [`PutTerminalError`]. Caps the DoS amplification surface when the
/// envelope flows multi-hop (via [`relay_put_send_error`]).
pub(crate) const PUT_TERMINAL_CAUSE_MAX_BYTES: usize = 2048;

/// UTF-8-safe length cap with an ASCII truncation marker.
///
/// # Multi-hop idempotency invariant
///
/// `bound_cause` is called at every hop a `PutMsg::Error` traverses
/// (loopback emit, `PutTerminalError::from_wire`, `relay_put_send_error`
/// bubble). The bubble must forward causes **verbatim-or-rebound,
/// never append**: any future relay that augments the cause (e.g.
/// prepending a `[hop=N]` tag) would otherwise cause this function to
/// re-truncate on each hop, stamping a fresh `...[truncated]` marker
/// every time and silently injecting double/triple markers into the
/// envelope the originator eventually classifies.
///
/// Today the call is idempotent on already-bounded short inputs (the
/// `<= PUT_TERMINAL_CAUSE_MAX_BYTES` early return), so the invariant
/// holds by construction. Don't break it without revisiting the
/// truncation-marker contract.
pub(crate) fn bound_cause(cause: String) -> String {
    const SUFFIX: &str = "...[truncated]";
    if cause.len() <= PUT_TERMINAL_CAUSE_MAX_BYTES {
        return cause;
    }
    let budget = PUT_TERMINAL_CAUSE_MAX_BYTES.saturating_sub(SUFFIX.len());
    // Walk back to a char boundary — `str` indexing must not split a codepoint.
    let mut cut = budget.min(cause.len());
    while cut > 0 && !cause.is_char_boundary(cut) {
        cut -= 1;
    }
    let mut out = String::with_capacity(cut + SUFFIX.len());
    out.push_str(&cause[..cut]);
    out.push_str(SUFFIX);
    out
}

/// In-process counterpart of the wire [`PutMsg::Error::cause`].
/// The wire side stays as a raw `String` for bincode compat;
/// `PutTerminalError` gives the retry-loop `Terminal` type a named
/// shape and a single point to enforce length caps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PutTerminalError {
    cause: String,
}

impl PutTerminalError {
    /// Applies [`bound_cause`] so multi-hop forwarding can't amplify
    /// attacker-controlled cause strings.
    pub(crate) fn from_wire(cause: String) -> Self {
        Self {
            cause: bound_cause(cause),
        }
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.cause
    }

    pub(crate) fn into_string(self) -> String {
        self.cause
    }
}

impl std::fmt::Display for PutTerminalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Telemetry data for originator-side PUT finalization.
pub(super) struct PutFinalizationData {
    pub sender: PeerKeyLocation,
    pub hop_count: Option<usize>,
    pub state_hash: Option<String>,
    pub state_size: Option<usize>,
}

/// Originator-side finalization after a PUT has been accepted by the network.
///
/// Emits `put_success` telemetry and, if `subscribe` is true,
/// starts a post-PUT subscription.
pub(super) async fn finalize_put_at_originator(
    op_manager: &OpManager,
    id: Transaction,
    key: ContractKey,
    telemetry: PutFinalizationData,
    subscribe: bool,
    blocking_subscribe: bool,
) {
    if let Some(event) = NetEventLog::put_success(
        &id,
        &op_manager.ring,
        key,
        telemetry.sender,
        telemetry.hop_count,
        telemetry.state_hash,
        telemetry.state_size,
    ) {
        op_manager.ring.register_events(Either::Left(event)).await;
    }

    // Mark the contract as locally-accessed now that the originator's PUT
    // has succeeded and the local cache entry exists (created earlier by
    // the put pipeline's `put_contract` + `host_contract`). Without this,
    // self-hosted contracts that were PUT'd by a local client but never
    // GET'd would never get the `local_client_access` flag — the GET path
    // is the only other production call site for `mark_local_client_access`.
    // Missing the flag excludes the contract from
    // `contracts_needing_renewal`, the subscription expires, the entry
    // eventually gets evicted under byte-budget pressure, and the next
    // cold remote GET fails the `is_locally_hosted` shortcut and routes
    // to the network — where, for a contract no other peer is subscribed
    // to, the GetOp hangs until the WS client times out. (freenet-stdlib
    // mirror demo, 2026-05-14: 180s timeouts on
    // freenet:96rknpy1GYhZ/freenet-stdlib for exactly this reason.)
    op_manager.ring.mark_local_client_access(&key);

    if subscribe {
        start_subscription_after_put(op_manager, id, key, blocking_subscribe).await;
    }
}

/// The `blocking_subscription` parameter controls subscription behavior:
/// - When false (default): subscription completes asynchronously and PUT response
///   is sent immediately
/// - When true: PUT response waits for subscription to complete
///
/// This value comes from the client request's `blocking_subscribe` field
/// (`ContractRequest::Put`).
async fn start_subscription_after_put(
    op_manager: &OpManager,
    parent_tx: Transaction,
    key: ContractKey,
    blocking_subscription: bool,
) {
    let child_tx =
        super::start_subscription_request(op_manager, parent_tx, key, blocking_subscription);
    tracing::debug!(
        tx = %parent_tx,
        child_tx = %child_tx,
        contract = %key,
        blocking = blocking_subscription,
        phase = "subscribe",
        "Started subscription after PUT"
    );
}

/// Stores the contract state and returns (new_state, state_changed).
/// `state_changed` is true if the stored state was actually modified
/// (old state != new state), which is needed to trigger UPDATE propagation.
pub(super) async fn put_contract(
    op_manager: &OpManager,
    key: ContractKey,
    state: WrappedState,
    related_contracts: RelatedContracts<'static>,
    contract: &ContractContainer,
) -> Result<(WrappedState, bool), OpError> {
    match op_manager
        .notify_contract_handler(ContractHandlerEvent::PutQuery {
            key,
            state,
            related_contracts,
            contract: Some(contract.clone()),
        })
        .await
    {
        Ok(ContractHandlerEvent::PutResponse {
            new_value: Ok(new_val),
            state_changed,
        }) => {
            op_manager.notify_contract_stored(&key);
            // Invariant: after a successful PUT the stored state must be non-empty.
            // A successful PutResponse with an empty value indicates a contract handler bug.
            debug_assert!(
                new_val.size() > 0,
                "put_contract: stored state must be non-empty after successful PUT for contract {key}"
            );
            Ok((new_val, state_changed))
        }
        Ok(ContractHandlerEvent::PutResponse {
            new_value: Err(err),
            ..
        }) => {
            // Issue #4251: per-contract queue saturation logs at DEBUG, not
            // ERROR — same rationale as the matching site in
            // `update.rs::log_update_contract_failure`.
            if err.is_contract_queue_full() {
                tracing::debug!(
                    contract = %key,
                    error = %err,
                    event = "queue_full",
                    "PUT skipped: per-contract queue saturated"
                );
            } else {
                tracing::error!(contract = %key, error = %err, phase = "error", "Failed to update contract value");
            }
            Err(OpError::from(err))
        }
        Err(err) => Err(err.into()),
        Ok(_) => Err(OpError::UnexpectedOpState),
    }
}

mod messages {
    use std::{collections::HashSet, fmt::Display};

    use freenet_stdlib::prelude::*;
    use serde::{Deserialize, Serialize};

    use crate::message::{InnerMessage, Transaction};
    use crate::ring::Location;
    use crate::transport::peer_connection::StreamId;

    /// Payload for streaming PUT requests.
    /// This struct is serialized and sent via the stream, while the metadata
    /// is sent via the RequestStreaming message.
    #[derive(Debug, Serialize, Deserialize)]
    pub(crate) struct PutStreamingPayload {
        pub contract: ContractContainer,
        #[serde(deserialize_with = "RelatedContracts::deser_related_contracts")]
        pub related_contracts: RelatedContracts<'static>,
        pub value: WrappedState,
    }

    /// PUT operation messages.
    ///
    /// The PUT operation stores a contract and its initial state in the network.
    /// It uses hop-by-hop routing: each node forwards toward the contract location
    /// and remembers where the request came from to route the response back.
    ///
    /// If a PUT reaches a node that is already subscribed to the contract and the
    /// merged state differs from the input, an Update operation is triggered to
    /// propagate the change to other subscribers.
    #[derive(Debug, Serialize, Deserialize, Clone)]
    #[non_exhaustive]
    pub(crate) enum PutMsg {
        /// Request to store a contract. Forwarded hop-by-hop toward contract location.
        /// Each receiving node:
        /// 1. Stores the contract locally (caching)
        /// 2. Forwards to the next hop closer to contract location
        /// 3. Remembers upstream_addr to route the response back
        Request {
            id: Transaction,
            contract: ContractContainer,
            #[serde(deserialize_with = "RelatedContracts::deser_related_contracts")]
            related_contracts: RelatedContracts<'static>,
            value: WrappedState,
            /// Hops to live - decremented at each hop, request fails if reaches 0
            htl: usize,
            /// Addresses to skip when selecting next hop (prevents loops)
            skip_list: HashSet<std::net::SocketAddr>,
        },
        /// Response indicating the PUT completed. Routed hop-by-hop back to originator
        /// using each node's stored upstream_addr.
        Response {
            id: Transaction,
            key: ContractKey,
            /// Forward-path hop count: how many hops the originating Request
            /// traversed before reaching the node that produced this Response
            /// (the final storer for `Response`, or the relay that finalised
            /// locally because it had no next hop).
            ///
            /// Computed as `max_hops_to_live - htl_at_responder`. The relay
            /// chain preserves this value as the Response bubbles back to the
            /// originator — it does NOT increment on the return path. This
            /// gives the whitepaper's "routing depth" metric (forward hops),
            /// not round-trip.
            ///
            /// `#[serde(default)]` is set for source-level clarity. Bincode
            /// does not honour serde defaults (positional encoding), so wire
            /// compat with peers that lack this field is handled at the
            /// handshake layer via `MIN_COMPATIBLE_VERSION`.
            ///
            /// Mirror of `GetMsg::Response.hop_count` (PR #4245); see also
            /// `SubscribeMsg::Response.hop_count`.
            #[serde(default)]
            hop_count: usize,
        },

        /// Streaming request to store a large contract. Used when payload exceeds
        /// streaming_threshold (default 64KB). The actual data is sent via a separate
        /// stream identified by stream_id.
        ///
        /// This variant is only used when streaming is enabled in config.
        RequestStreaming {
            id: Transaction,
            /// Identifies the stream carrying the contract and state data
            stream_id: StreamId,
            /// Key of the contract being stored
            contract_key: ContractKey,
            /// Total size of the streamed payload in bytes
            total_size: u64,
            /// Hops to live - decremented at each hop
            htl: usize,
            /// Addresses to skip when selecting next hop
            skip_list: HashSet<std::net::SocketAddr>,
            /// Whether to subscribe to updates after storing
            subscribe: bool,
        },

        /// Streaming response indicating PUT completed for a streaming request.
        /// Sent back to the originator after the stream has been fully received
        /// and the contract stored.
        ResponseStreaming {
            id: Transaction,
            key: ContractKey,
            /// Whether the receiving node should continue forwarding to other peers
            continue_forwarding: bool,
            /// Forward-path hop count — same semantics as
            /// `PutMsg::Response.hop_count`. Carried for wire-format
            /// symmetry: production code currently downgrades streaming
            /// replies to non-streaming `Response` at the relay
            /// (see `op_ctx_task::drive_relay_put` slice A note), but the
            /// field is preserved here so any future streaming-passthrough
            /// path can populate it without another wire bump.
            #[serde(default)]
            hop_count: usize,
        },

        /// Lightweight ACK sent by a relay peer back to its upstream when it forwards
        /// a PUT request to the next hop. Tells the upstream "I received the data and
        /// am processing it" so the GC task can distinguish dead peers from slow
        /// multi-hop chains. Fire-and-forget — no response expected.
        ForwardingAck {
            id: Transaction,
            contract_key: ContractKey,
        },

        /// Terminal failure delivered to the originator's driver via the
        /// same `pending_op_results` bypass as `Response`. Carries the
        /// contract-side or local-validation reason as a string so the
        /// originator's `start_client_put` publishes a single
        /// `HostResult::Err(OperationError { cause })` instead of
        /// burning the retry budget on a deterministic failure and
        /// racing the genuine reason against the synthesised
        /// "failed notifying, channel closed" marker.
        ///
        /// Constructed by `run_relay_put` in the originator-loopback
        /// failure path; bubbled up multi-hop chains by
        /// `relay_put_send_error` (see [`op_ctx_task`]).
        Error {
            id: Transaction,
            /// Human-readable failure reason surfaced to the client.
            /// Kept as `String` (not `ClientError` / `OpError`) so the
            /// wire variant stays dependency-free for `bincode` —
            /// future serde changes in those types don't break wire
            /// compatibility. Truncated to
            /// [`PUT_TERMINAL_CAUSE_MAX_BYTES`] via [`bound_cause`] at
            /// every entry point. Wrapped into
            /// [`super::PutTerminalError`] for in-process
            /// classification.
            cause: String,
        },
    }

    impl InnerMessage for PutMsg {
        fn id(&self) -> &Transaction {
            match self {
                Self::Request { id, .. }
                | Self::Response { id, .. }
                | Self::RequestStreaming { id, .. }
                | Self::ResponseStreaming { id, .. }
                | Self::ForwardingAck { id, .. }
                | Self::Error { id, .. } => id,
            }
        }

        fn requested_location(&self) -> Option<Location> {
            match self {
                Self::Request { contract, .. } => Some(Location::from(contract.id())),
                Self::Response { key, .. } => Some(Location::from(key.id())),
                Self::RequestStreaming { contract_key, .. } => {
                    Some(Location::from(contract_key.id()))
                }
                Self::ResponseStreaming { key, .. } => Some(Location::from(key.id())),
                Self::ForwardingAck { contract_key, .. } => Some(Location::from(contract_key.id())),
                // No contract key in the failure envelope — the originator
                // already knows the key it requested; the failure is keyed
                // by tx only.
                Self::Error { .. } => None,
            }
        }
    }

    impl Display for PutMsg {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Request {
                    id, contract, htl, ..
                } => {
                    write!(
                        f,
                        "PutRequest(id: {}, key: {}, htl: {})",
                        id,
                        contract.key(),
                        htl
                    )
                }
                Self::Response { id, key, .. } => {
                    write!(f, "PutResponse(id: {}, key: {})", id, key)
                }
                Self::RequestStreaming {
                    id,
                    stream_id,
                    contract_key,
                    total_size,
                    htl,
                    ..
                } => {
                    write!(
                        f,
                        "PutRequestStreaming(id: {}, key: {}, stream: {}, size: {}, htl: {})",
                        id, contract_key, stream_id, total_size, htl
                    )
                }
                Self::ResponseStreaming {
                    id,
                    key,
                    continue_forwarding,
                    ..
                } => {
                    write!(
                        f,
                        "PutResponseStreaming(id: {}, key: {}, continue: {})",
                        id, key, continue_forwarding
                    )
                }
                Self::ForwardingAck { id, contract_key } => {
                    write!(f, "PutForwardingAck(id: {}, key: {})", id, contract_key)
                }
                Self::Error { id, cause } => {
                    write!(f, "PutError(id: {}, cause: {})", id, cause)
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::wildcard_enum_match_arm)]
mod tests {
    use super::*;
    use crate::message::{InnerMessage, Transaction};
    use crate::operations::test_utils::make_contract_key;

    #[test]
    fn put_msg_id_returns_transaction() {
        let tx = Transaction::new::<PutMsg>();
        let msg = PutMsg::Response {
            id: tx,
            key: make_contract_key(1),
            hop_count: 0,
        };
        assert_eq!(*msg.id(), tx, "id() should return the transaction ID");
    }

    #[test]
    fn put_msg_display_formats_correctly() {
        let tx = Transaction::new::<PutMsg>();
        let msg = PutMsg::Response {
            id: tx,
            key: make_contract_key(1),
            hop_count: 0,
        };
        let display = format!("{}", msg);
        assert!(
            display.contains("PutResponse"),
            "Display should contain message type name"
        );
    }

    /// `PutMsg::Error` honours `id()` and `Display` like every other
    /// variant.
    #[test]
    fn put_msg_error_id_and_display() {
        let tx = Transaction::new::<PutMsg>();
        let msg = PutMsg::Error {
            id: tx,
            cause: "contract rejected: version must be higher".into(),
        };
        assert_eq!(*msg.id(), tx, "id() should return the transaction ID");
        let display = format!("{}", msg);
        assert!(display.contains("PutError"), "Display tag");
        assert!(display.contains("version must be higher"), "Display cause");
    }

    /// `PutMsg::Error` round-trips through bincode intact.
    #[test]
    fn put_msg_error_serde_roundtrip() {
        let tx = Transaction::new::<PutMsg>();
        let cause = "execution error: invalid contract update".to_string();
        let msg = PutMsg::Error {
            id: tx,
            cause: cause.clone(),
        };

        let serialized = bincode::serialize(&msg).expect("serialize");
        let deserialized: PutMsg = bincode::deserialize(&serialized).expect("deserialize");

        match deserialized {
            PutMsg::Error {
                id,
                cause: decoded_cause,
            } => {
                assert_eq!(id, tx);
                assert_eq!(decoded_cause, cause);
            }
            other => panic!("Expected Error, got {other}"),
        }
    }

    /// `Error` is keyed by tx only — `requested_location` must not
    /// gain a phantom location.
    #[test]
    fn put_msg_error_has_no_requested_location() {
        let tx = Transaction::new::<PutMsg>();
        let msg = PutMsg::Error {
            id: tx,
            cause: "x".into(),
        };
        assert!(msg.requested_location().is_none());
    }

    /// Wire-format pin: bincode encodes `PutMsg` variants as a u32 LE
    /// tag in declaration order. Reordering or inserting before an
    /// existing variant is a silent wire-break against deployed peers.
    /// `#[non_exhaustive]` only protects Rust match arms; tag
    /// stability is enforced here.
    ///
    /// **Codec-config assumption.** This test uses default
    /// `bincode::serialize`, which currently emits enum variant tags
    /// as fixed-width `u32` little-endian. The test invariants
    /// (variant tag == declaration index, encoded length ≥ 4 bytes)
    /// depend on that. If the project ever switches bincode codec
    /// config — e.g. to `with_varint_encoding()` (variable-length
    /// tags, breaks length assumption) or `with_big_endian()`
    /// (breaks LE assumption) or to bincode 2's `standard()` config
    /// — this pin must be updated in lockstep, otherwise the wire
    /// format silently changes against deployed peers without
    /// tripping any test. The single source of truth for the codec
    /// config is the deserialize site in the transport stack; keep
    /// that and this test in sync.
    #[test]
    fn put_msg_wire_format_variant_tags_are_stable() {
        let tx = Transaction::new::<PutMsg>();
        let key = make_contract_key(0);
        // One minimal instance per variant, in declaration order.
        let samples: [(u32, PutMsg); 6] = [
            (
                0,
                PutMsg::Request {
                    id: tx,
                    contract: crate::operations::test_utils::make_test_contract(&[]),
                    related_contracts: RelatedContracts::default(),
                    value: WrappedState::new(vec![]),
                    htl: 0,
                    skip_list: Default::default(),
                },
            ),
            (
                1,
                PutMsg::Response {
                    id: tx,
                    key,
                    hop_count: 0,
                },
            ),
            (
                2,
                PutMsg::RequestStreaming {
                    id: tx,
                    stream_id: crate::transport::StreamId::next(),
                    contract_key: key,
                    total_size: 0,
                    htl: 0,
                    skip_list: Default::default(),
                    subscribe: false,
                },
            ),
            (
                3,
                PutMsg::ResponseStreaming {
                    id: tx,
                    key,
                    continue_forwarding: false,
                    hop_count: 0,
                },
            ),
            (
                4,
                PutMsg::ForwardingAck {
                    id: tx,
                    contract_key: key,
                },
            ),
            (
                5,
                PutMsg::Error {
                    id: tx,
                    cause: String::new(),
                },
            ),
        ];
        for (expected_tag, msg) in samples {
            let bytes = bincode::serialize(&msg).expect("serialize");
            assert!(
                bytes.len() >= 4,
                "encoded PutMsg too short to carry a tag: {msg}"
            );
            let actual_tag = u32::from_le_bytes(bytes[..4].try_into().expect("first 4 bytes"));
            assert_eq!(
                actual_tag, expected_tag,
                "PutMsg wire tag for `{msg}` shifted — reordering variants is a wire-format \
                 break. If you intentionally renumbered, bump the freenet-stdlib major and \
                 coordinate the upgrade."
            );
        }
    }

    /// `bound_cause` keeps short strings byte-identical and truncates
    /// long strings to `PUT_TERMINAL_CAUSE_MAX_BYTES` with a
    /// `...[truncated]` suffix. Used as the DoS-amplification guard
    /// when `PutMsg::Error` flows multi-hop.
    #[test]
    fn bound_cause_short_string_passes_through() {
        let s = "execution error: contract rejected".to_string();
        assert_eq!(bound_cause(s.clone()), s);
    }

    #[test]
    fn bound_cause_truncates_oversize_at_char_boundary() {
        let s = "a".repeat(PUT_TERMINAL_CAUSE_MAX_BYTES * 2);
        let bounded = bound_cause(s);
        assert!(bounded.len() <= PUT_TERMINAL_CAUSE_MAX_BYTES);
        assert!(bounded.ends_with("...[truncated]"));
    }

    #[test]
    fn bound_cause_never_splits_utf8_codepoint() {
        // PR #4126 review item M2: the previous version of this test
        // used `"好".repeat(_)` — 3-byte codepoints. The internal
        // budget `PUT_TERMINAL_CAUSE_MAX_BYTES - "...[truncated]".len()`
        // is currently `2048 - 14 = 2034`, and `2034 % 3 == 0`, so
        // the cut landed exactly on a codepoint boundary and the
        // `while !is_char_boundary(cut)` walk-back loop ran zero
        // iterations. The "never splits" guarantee was therefore
        // never actually exercised.
        //
        // Switch to a 4-byte codepoint (U+1D11E "𝄞", G clef) sized so
        // the cap lands mid-codepoint, forcing the walk-back to
        // execute. `2034 % 4 == 2` → byte 2034 is two bytes into a
        // 4-byte codepoint, so `is_char_boundary(2034)` MUST be false
        // and the loop MUST trim at least one byte.
        const FOUR_BYTE: &str = "𝄞"; // U+1D11E, 4 bytes UTF-8
        assert_eq!(FOUR_BYTE.len(), 4);

        // 600 × 4 = 2400 bytes, well over the cap.
        let n_codepoints = 600;
        let s: String = FOUR_BYTE.repeat(n_codepoints);
        assert_eq!(s.len(), 4 * n_codepoints);

        // Sanity: the raw budget cut WOULD split a codepoint on this
        // input. If this assertion ever stops holding (e.g. someone
        // changes the cap or the suffix length so the budget realigns
        // on 4), this test is back to being a no-op and needs to
        // pick a different codepoint width.
        let suffix_len = "...[truncated]".len();
        let raw_budget = PUT_TERMINAL_CAUSE_MAX_BYTES.saturating_sub(suffix_len);
        assert!(
            !s.is_char_boundary(raw_budget),
            "test invariant: raw budget {raw_budget} must land mid-codepoint \
             for the walk-back loop to be exercised — pick a codepoint width \
             coprime with the budget"
        );

        let bounded = bound_cause(s);
        assert!(
            bounded.is_char_boundary(bounded.len()),
            "bounded output must end on a UTF-8 codepoint boundary"
        );
        // Strict-less-than is the proof that the walk-back actually
        // ran: had it taken zero iterations the output length would
        // equal exactly `PUT_TERMINAL_CAUSE_MAX_BYTES`. A trimmed
        // walk-back leaves bounded.len() < cap.
        assert!(
            bounded.len() < PUT_TERMINAL_CAUSE_MAX_BYTES,
            "walk-back must have trimmed at least one byte (cap={}, \
             actual bounded.len()={})",
            PUT_TERMINAL_CAUSE_MAX_BYTES,
            bounded.len()
        );
        assert!(bounded.ends_with("...[truncated]"));
        // Re-parsing must not panic.
        let _ = bounded.chars().count();
    }

    /// PR #4126 review minor: pin the off-by-one boundary around the
    /// cap. `bound_cause` MUST be byte-identical for input of length
    /// `PUT_TERMINAL_CAUSE_MAX_BYTES` (the if-guard takes `<=`), and
    /// MUST truncate (with suffix) starting at `+1`.
    #[test]
    fn bound_cause_cap_boundary_is_inclusive() {
        // Exactly at the cap → no truncation.
        let exact = "a".repeat(PUT_TERMINAL_CAUSE_MAX_BYTES);
        let bounded = bound_cause(exact.clone());
        assert_eq!(
            bounded, exact,
            "input of exactly PUT_TERMINAL_CAUSE_MAX_BYTES bytes must pass \
             through verbatim — the cap is INCLUSIVE"
        );

        // One byte over → truncation with suffix.
        let one_over = "a".repeat(PUT_TERMINAL_CAUSE_MAX_BYTES + 1);
        let bounded = bound_cause(one_over);
        assert!(
            bounded.ends_with("...[truncated]"),
            "input of PUT_TERMINAL_CAUSE_MAX_BYTES + 1 bytes MUST be \
             truncated with the suffix marker"
        );
        assert!(
            bounded.len() <= PUT_TERMINAL_CAUSE_MAX_BYTES,
            "truncated output MUST fit within the cap"
        );
    }

    /// `PutTerminalError::from_wire` applies `bound_cause`, so an
    /// attacker-controlled upstream cannot inflate the per-hop cost
    /// of `PutMsg::Error` by emitting multi-MB causes.
    #[test]
    fn put_terminal_error_from_wire_truncates() {
        let oversize = "x".repeat(PUT_TERMINAL_CAUSE_MAX_BYTES * 3);
        let err = PutTerminalError::from_wire(oversize);
        assert!(err.as_str().len() <= PUT_TERMINAL_CAUSE_MAX_BYTES);
        assert!(err.as_str().ends_with("...[truncated]"));
    }

    #[test]
    fn test_forwarding_ack_serde_roundtrip() {
        let tx = Transaction::new::<PutMsg>();
        let key = make_contract_key(42);
        let msg = PutMsg::ForwardingAck {
            id: tx,
            contract_key: key,
        };

        let serialized = bincode::serialize(&msg).expect("serialize");
        let deserialized: PutMsg = bincode::deserialize(&serialized).expect("deserialize");

        match deserialized {
            PutMsg::ForwardingAck { id, contract_key } => {
                assert_eq!(id, tx);
                assert_eq!(contract_key, key);
            }
            other => panic!("Expected ForwardingAck, got {other}"),
        }
    }

    /// Regression test: `PutMsg::Response.hop_count` and
    /// `PutMsg::ResponseStreaming.hop_count` roundtrip through bincode.
    ///
    /// Before #4248 the PUT telemetry path computed `hop_count` at log time
    /// via `op_manager.get_current_hop(id)`, which returned `None` once the
    /// operation had been cleaned up — i.e., on the vast majority of
    /// terminal PUT events.  The fix carries the value on the wire so the
    /// originator has it when constructing `PutSuccess` log events.  This
    /// test asserts that the new field survives round-trip serialisation
    /// for both `Response` and `ResponseStreaming` variants — i.e., the
    /// wire format actually carries it.
    ///
    /// bincode-positional caveat: any future positional change here will
    /// break older binaries; see the `MIN_COMPATIBLE_VERSION` bump that
    /// accompanies this PR.
    #[test]
    fn test_put_msg_response_hop_count_roundtrip() {
        let key = make_contract_key(7);
        let cases: &[(&str, usize)] = &[
            ("zero", 0),
            ("one", 1),
            ("mid", 4),
            ("htl", 10),
            ("large", 64),
        ];
        for (label, hop_count) in cases.iter().copied() {
            // Response variant
            let response = PutMsg::Response {
                id: Transaction::new::<PutMsg>(),
                key,
                hop_count,
            };
            let bytes = bincode::serialize(&response).expect(label);
            let restored: PutMsg = bincode::deserialize(&bytes).expect(label);
            match restored {
                PutMsg::Response { hop_count: hc, .. } => {
                    assert_eq!(hc, hop_count, "Response.hop_count must roundtrip ({label})")
                }
                _ => panic!("expected Response for {label}"),
            }

            // ResponseStreaming variant
            let streaming = PutMsg::ResponseStreaming {
                id: Transaction::new::<PutMsg>(),
                key,
                continue_forwarding: false,
                hop_count,
            };
            let bytes = bincode::serialize(&streaming).expect(label);
            let restored: PutMsg = bincode::deserialize(&bytes).expect(label);
            match restored {
                PutMsg::ResponseStreaming { hop_count: hc, .. } => assert_eq!(
                    hc, hop_count,
                    "ResponseStreaming.hop_count must roundtrip ({label})"
                ),
                _ => panic!("expected ResponseStreaming for {label}"),
            }
        }
    }

    /// Pin: the PUT GC speculative retry accumulator and retry-count
    /// map must not return. Their reintroduction risks the per-tx
    /// DashMap leak the surrounding code was rebuilt to avoid.
    #[test]
    fn put_gc_speculative_retry_block_must_stay_deleted() {
        let src = include_str!("../node/op_state_manager.rs");
        assert!(
            !src.contains("put_retry_candidates"),
            "`put_retry_candidates` accumulator must stay deleted"
        );
        assert!(
            !src.contains("put_retried"),
            "`put_retried` retry-count map must stay deleted"
        );
    }

    /// Pin: `OpManager::completed` must not touch any per-op DashMap.
    /// The surviving completion side effects are limited to the global
    /// `under_progress` / `completed` sets and the `request_router`.
    #[test]
    fn completed_must_not_touch_per_op_dashmaps() {
        let src = include_str!("../node/op_state_manager.rs");
        let fn_start = src
            .find("pub fn completed(&self, id: Transaction)")
            .expect("OpManager::completed not found");
        let fn_end = src[fn_start..]
            .find("\n    }\n")
            .expect("OpManager::completed closing brace not found")
            + fn_start;
        let body = &src[fn_start..fn_end];
        for forbidden in [
            "self.ops.connect",
            "self.ops.put",
            "self.ops.get",
            "self.ops.subscribe",
            "self.ops.update",
        ] {
            assert!(
                !body.contains(forbidden),
                "OpManager::completed must not reference `{forbidden}`"
            );
        }
    }

    /// Pin: ForwardingAck senders must not return. Relay drivers omit
    /// them (would race the capacity-1 reply waiter) and the consumer
    /// is a no-op.
    #[test]
    fn put_forwarding_ack_senders_must_stay_deleted() {
        let src = include_str!("put.rs");
        let needle = format!("NetMessage::from({}::ForwardingAck", "PutMsg",);
        assert!(
            !src.contains(&needle),
            "ForwardingAck senders must not be reintroduced"
        );
    }

    /// Regression guard for the freenet-stdlib mirror demo 180s timeout
    /// (2026-05-14). `finalize_put_at_originator` MUST call the
    /// originator-side mark so PUT'd-but-never-GET'd contracts get the
    /// local-client flag set on their hosting cache entry. Without this,
    /// `contracts_needing_renewal` excludes them, the subscription
    /// expires, the entry eventually gets evicted, and the next cold
    /// remote GET fails the `is_locally_hosted` shortcut and routes to
    /// the network — where, for a contract no other peer subscribes to,
    /// the GetOp hangs until the WS client's 180s timeout fires.
    ///
    /// Implementation note: matches on the exact executable call syntax
    /// AFTER stripping line comments, so the assertion can't be satisfied
    /// by a doc comment that merely mentions the function name (Codex
    /// caught this in PR #4133's first review iteration).
    #[test]
    fn finalize_put_at_originator_marks_local_client_access() {
        const SOURCE: &str = include_str!("put.rs");

        let fn_start = SOURCE
            .find("pub(super) async fn finalize_put_at_originator(")
            .expect("finalize_put_at_originator definition not found");
        let body_start = SOURCE[fn_start..]
            .find('{')
            .map(|p| fn_start + p)
            .expect("function body opening brace not found");
        let body_end = SOURCE[body_start..]
            .find("\n}\n")
            .map(|p| body_start + p)
            .expect("function body closing brace not found");
        let body = &SOURCE[body_start..body_end];

        // Strip line comments before matching so a doc comment that
        // mentions the function name doesn't false-pass the assertion.
        let executable: String = body
            .lines()
            .map(|line| line.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");

        assert!(
            executable.contains("ring.mark_local_client_access(&key)"),
            "finalize_put_at_originator MUST call \
             `op_manager.ring.mark_local_client_access(&key)` (executable \
             code, not just a comment mention) so self-hosted contracts \
             get the local-client flag set. Without this call, the \
             freenet-stdlib mirror demo 180s cold-GET timeout (2026-05-14) \
             returns. See contracts_needing_renewal at hosting.rs:971-991 \
             for the downstream gate that depends on this flag."
        );
    }
}
