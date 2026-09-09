//! Parking a delegate's round-trip OFF the serial `contract_handling` loop
//! (#5544).
//!
//! # The problem
//!
//! A delegate round-trip is already a two-invocation protocol at the runtime
//! level: `RequestUserInput` breaks out of `process_outbound`, the WASM
//! `process()` call returns, and the delegate's continuation lives in the
//! [`DelegateContextCache`](crate::wasm_runtime::native_api::DelegateContextCache)
//! until the executor re-enters with the matching `UserResponse`.
//!
//! What made that round-trip look atomic was that
//! `handle_delegate_with_contract_requests` awaited the slow half INLINE, on
//! the single serial loop — so nothing else on the node ran until it finished.
//! For a permission prompt that is up to `USER_INPUT_TIMEOUT` (60 s) during
//! which no GET, PUT, UPDATE, subscribe or delegate notification is serviced
//! anywhere on this node.
//!
//! Parking replaces that: the loop hands the slow half to a spawned task and
//! returns immediately, and the delegate is re-entered on a later iteration
//! when the result arrives.
//!
//! # Why this needs per-delegate exclusion
//!
//! The context cache is keyed by `DelegateKey` alone and is last-write-wins.
//! It is only sound while at most ONE `process()` per delegate is in flight,
//! and — see that type's rustdoc, corrected in this same change — that
//! property is supplied ENTIRELY by the serial loop, not by the runtime. There
//! is no per-delegate lock in `prepare_delegate_call`, no per-delegate
//! affinity in `RuntimePool::execute_delegate_request`, and no mutex anywhere
//! in `wasm_runtime::delegate`.
//!
//! So the moment a round-trip spans two loop iterations, that protection is
//! gone: a second request for the same delegate would run `process()`, write
//! the shared context, and the parked continuation would resume reading
//! someone else's bytes. Silent state corruption, not a crash.
//!
//! [`DelegateParkCtx`] therefore keeps its own per-delegate exclusion: while a
//! delegate is parked, further requests for it are QUEUED rather than run, and
//! drained when it resumes. That preserves the invariant the delegate author
//! already relies on, and converts a node-wide stall into a per-delegate one —
//! which is the correct semantics, not a compromise. Everything else on the
//! node keeps running.
//!
//! # What parking does NOT relax: `process()` stays globally serial
//!
//! Parking releases the loop while a delegate is **suspended**, never while it
//! is **running**. That distinction is load-bearing for code outside this
//! module, so state it as an invariant:
//!
//! > **At most one delegate `process()` executes node-wide at any instant, and
//! > it always executes on the `contract_handling` loop.**
//!
//! Two separate properties depend on it:
//!
//! * `DelegateContextCache` needs one `process()` **per delegate** — that is
//!   the narrower guarantee, and it is the one [`DelegateParkCtx`]'s exclusion
//!   supplies, because parking genuinely does let a delegate's round-trip span
//!   loop iterations.
//! * `native_api::state_content_changed` (V2 delegate writes, #5490) needs one
//!   write **per contract**. Its read-then-write pair is not atomic, and its
//!   racing pair is two DIFFERENT delegates writing the SAME contract — which
//!   per-delegate exclusion permits by construction. It is safe only because of
//!   the global property above, not because of anything in this module.
//!
//! Why the global property still holds after #5544, by construction rather than
//! by convention:
//!
//! 1. `execute_delegate_request` is reached ONLY through
//!    `handle_delegate_with_contract_requests`.
//! 2. Every route into that function is awaited, directly or one hop removed,
//!    from `contract_handling` — a single task per node:
//!
//!    ```text
//!    contract_handling
//!      ├─ handle_contract_event ─────────► dispatch_delegate_request ─┐
//!      ├─ handle_delegate_notification ────────────────────────────────┤
//!      └─ handle_delegate_resume ──┬───────────────────────────────────┤
//!                                  ├─► dispatch_delegate_request ──────┤
//!                                  └─► run_queued_notification ────────┘
//!                                                                      │
//!                          handle_delegate_with_contract_requests ◄─────┘
//!                                      └─► execute_delegate_request
//!    ```
//!
//!    Stated as the PROPERTY — every route is awaited from the one loop — and
//!    not as a count. An exact tally is a fact with an expiry date: this list
//!    said "four call sites" until `run_queued_notification` was added for the
//!    notification-coalescing fix, and was wrong the moment it was. The
//!    property survives a new caller; the number does not. If you add a route,
//!    it must be awaited from this loop or the invariant is gone.
//! 3. The off-loop task this module spawns captures a `ParkGuard`, an
//!    `Arc<P: UserInputPrompter>`, an `Option<Arc<OpManager>>` and plain data.
//!    It does **not** capture the `ContractHandler` or an executor, so it
//!    cannot invoke a delegate even by mistake. Its two jobs — waiting on a
//!    human and driving a sub-op GET — need neither.
//! 4. A resume re-enters the delegate from `handle_delegate_resume`, which runs
//!    **on the loop**. The spawned task only ships a result back down a
//!    channel; it never runs the continuation itself.
//!
//! So the window parking opens is a window in which a *different* delegate may
//! **start**, not one in which two may **run**. #5490's TOCTOU stays
//! unreachable, and its atomic compare-and-write (folding the comparison into
//! the same ReDb write transaction as the store, the way `update_state_sync`
//! already does) is a follow-up rather than a prerequisite for this change.
//!
//! **What would break it.** Spawning any work that holds the
//! `ContractHandler`, or resuming a continuation anywhere other than the loop.
//! If you are about to do either, #5490's gate must become atomic first. The
//! nearest existing precedent is deliberately NOT a counter-example: #4531's
//! hosted-secret export does run off-loop holding a pooled executor, but it
//! enumerates and seals secrets and never invokes a delegate.
//!
//! **The pattern worth noticing.** This is the second documented-but-unenforced
//! invariant found to be resting on the serial loop by accident rather than by
//! design — the context cache was the first. Neither said so where it was
//! relied upon. When touching this loop, assume there is a third.
//!
//! # Ownership
//!
//! Loop-owned (`contract_handling` holds it and passes `&mut` down), NOT a
//! process global. It holds this node's client responders and gates this
//! node's loop, and an in-process multi-node simulation must not share either.
//! Same reasoning as `client_events::user_op_rate_limit`, and deliberately
//! unlike the older `DELEGATE_SUBSCRIPTIONS` global.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use either::Either;
use freenet_stdlib::client_api::DelegateRequest;
use freenet_stdlib::prelude::{
    ContractContainer, ContractInstanceId, ContractKey, DelegateContext, DelegateKey,
    InboundDelegateMsg, OutboundDelegateMsg, Parameters, RelatedContracts, StateDelta, UpdateData,
    WrappedState,
};

use super::executor::ExecutorError;

use super::handler::{EventId, StashedResponder};
use crate::client_events::ConnectionScope;
use crate::wasm_runtime::UserSecretContext;

/// Node-wide cap on simultaneously parked delegates.
///
/// Each park holds a continuation, at most one stashed client responder and up
/// to [`MAX_PENDING_PER_DELEGATE`] queued requests, so this bounds the whole
/// structure's footprint. 64 is far above any realistic concurrent count — a
/// node runs a handful of registered delegates and a prompt needs a human — and
/// well below anything that would matter for memory.
///
/// At the cap a new park is REFUSED and the caller falls back to answering the
/// delegate inline (the pre-#5544 behaviour, stall included) rather than
/// dropping the round-trip. Degrading to the old behaviour under an
/// implausible flood is strictly better than losing a user's prompt.
pub(super) const MAX_PARKED_DELEGATES: usize = 64;

/// Cap on requests queued behind a single parked delegate.
///
/// Overflow is REJECTED, and the caller's responder is DROPPED so the client
/// sees an error. It must NOT be answered with an empty `DelegateResponse`:
/// that is what a delegate which ran and said nothing returns, so it would
/// report success for work `process()` never performed. An earlier version of
/// this comment described exactly that rejected behaviour — the code, and
/// `a_request_refused_behind_a_full_pending_queue_errors_not_succeeds`, do the
/// opposite. A delegate with 8 requests already queued behind a prompt is not
/// going to be helped by a ninth.
pub(super) const MAX_PENDING_PER_DELEGATE: usize = 8;

/// Cap on DISTINCT contracts with a coalesced notification pending behind one
/// park.
///
/// Notifications coalesce per contract, so this bounds the map by the number of
/// contracts a delegate subscribes to rather than by message rate. In principle
/// #5493 bounds subscriptions separately; this cap does not assume that has
/// landed, because "bounded somewhere else" is the assumption that produced
/// three wrong-scope bounds on this change already. Over the cap the NEW
/// notification is dropped — the delegate will see that contract's next state
/// change, which is the pipeline's standing contract.
///
/// 16 rather than something larger because this lane also sets the worst-case
/// burst when a park is torn down: one resume runs `1 +
/// MAX_PENDING_PER_DELEGATE + MAX_PENDING_NOTIFICATION_CONTRACTS` delegate runs
/// before the fair queue gets a turn (#5544 M6). At 16 that is 25, against a
/// `MAX_RESUME_DRAIN_BATCH` of 16 — one over-long batch at park tear-down,
/// which cannot repeat until another park forms. At 64 it was 73.
pub(super) const MAX_PENDING_NOTIFICATION_CONTRACTS: usize = 16;

/// Cap on deferred related-contract fetches a single park may carry (#5544 S3).
///
/// The client-driven path bounds its off-loop fetches with
/// `MAX_INFLIGHT_DEFERRALS` (256) as explicit anti-amplification. The delegate
/// path cannot consult that counter — it has no `DeferralCtx` — and nothing
/// caps how many `PutContractRequest`s one `process()` may emit, each able to
/// name up to `MAX_RELATED_CONTRACTS_PER_REQUEST` (10) missing contracts.
///
/// 4 is chosen so the node-wide worst case is of the same ORDER as the client
/// path rather than a multiple of it: MAX_PARKED_DELEGATES (64) x 4 x 10 =
/// 2560 ids in flight, against the client path's 256 x 10 = 2560.
///
/// Read as "matches", which it was, that is a stronger claim than the numbers
/// support: the two are ADDITIVE, not alternatives — a node can be running the
/// client path's 2560 and this path's 2560 at once — so the real effect of
/// choosing 4 is to double a pre-existing ceiling rather than to stay under it.
/// Still the right value, for the reason below; the parity was never the
/// argument. Over the cap the excess upserts
/// fall back to the inline fetch, which stalls the loop for those specific
/// operations — the same deliberate trade as the park-cap fallback: degrading
/// to the old behaviour beats dropping a delegate's write.
pub(super) const MAX_DEFERRED_UPSERTS_PER_PARK: usize = 4;

/// Node-wide cap on the bytes a park may hold (#5544 S4).
///
/// `MAX_PARKED_DELEGATES` bounds the NUMBER of parks, which is not the same as
/// bounding their footprint: `Continuation::inbound_so_far` carries
/// `GetContractResponse`s holding full `WrappedState`s, so a count cap reads
/// like a memory bound and is not one. This is the fourth instance of that
/// pattern found on this change alone (see #5551), and `code-style.md` rule 4
/// requires the cap be on the quantity actually consumed.
///
/// 64 MiB on a host with the RAM for it — generous beside the 50 MB
/// single-state ceiling the runtime already allows, while bounding the
/// aggregate a flood of parked delegates can pin. See [`parked_budget_for`]:
/// this is the CLAMP CEILING, not the budget a given node gets.
pub(super) const MAX_PARKED_BYTES: usize = 64 * 1024 * 1024;

/// Floor, so a very small host still admits some parks rather than falling back
/// inline for everything.
///
/// An eighth of [`MAX_PARKED_BYTES`], and stated as such rather than left as a
/// bare number: with `PARKED_RAM_DIVISOR` of 32 it binds below 256 MiB of RAM,
/// and it is what [`upsert_fetch_allowance`] is smallest against — 512 KiB per
/// deferred upsert at the floor. Changing either without the other silently
/// moves where deferred upserts start degrading to the inline path.
const MIN_PARKED_BYTES: usize = MAX_PARKED_BYTES / 8;

/// RAM assumed when the host's real figure cannot be read — the same
/// conservative 1 GiB every sibling budget falls back to.
const PARKED_FALLBACK_TOTAL_RAM_BYTES: usize = 1024 * 1024 * 1024;

/// Fraction of node memory the park registry may pin. Matches the shape of
/// every sibling budget (`budget_for_ram`, `summary_budget_for`, ...), which is
/// the point — see [`parked_budget_for`].
const PARKED_RAM_DIVISOR: usize = 32;

/// The park budget a node with `total_ram` bytes actually gets.
///
/// A FLAT CONSTANT IS NOT A MEMORY BUDGET ON A SMALL HOST, and this only became
/// visible once `MAX_PARKED_BYTES` was added to
/// `contract::executor::declared_cache_ceiling` (#5554 follow-up). It had never
/// been in that sum, so nothing compared it against the host: with it included,
/// `cache_byte_budgets_are_aggregate_safe` immediately went red — a 1 GiB VPS
/// declared 566 MB of ceilings against a 537 MB half-limit. The over-commit was
/// real all along; the aggregate simply could not see it.
///
/// This is the same family of mistake as the one `MAX_PARKED_BYTES` itself was
/// introduced to fix, one level up: a count cap reads like a memory bound, and
/// a FLAT memory cap reads like it scales. Every other term in that sum is
/// RAM-derived, so this one is too.
pub(super) fn parked_budget_for(total_ram: usize) -> usize {
    (total_ram / PARKED_RAM_DIVISOR).clamp(MIN_PARKED_BYTES, MAX_PARKED_BYTES)
}

/// Bytes ONE deferred upsert's off-loop related-contract fetch may RETAIN.
///
/// This closes the largest hole in the byte bound, and the hole was total: the
/// off-loop fetch pushed every fetched `WrappedState` into the resume sink and
/// then an unbounded channel with **no accounting of any kind** — no reserve,
/// no check, no rejection path. `missing` is capped at
/// `MAX_RELATED_CONTRACTS_PER_REQUEST` (10) and a state at `MAX_STATE_SIZE`
/// (50 MiB), with `MAX_DEFERRED_UPSERTS_PER_PARK` (4) upserts per park, so one
/// park could retain 4 x 10 x 50 MiB ~= 2 GiB against a nominal 64 MiB cap. On
/// that path the cap did not bind at all.
///
/// WHY AN ALLOWANCE RATHER THAN A WORST-CASE RESERVE. Reserving what the fetch
/// COULD retrieve means reserving 2 GiB against 64 MiB, so every deferred
/// upsert naming a missing related contract would be refused and fall back to
/// the inline path — undoing much of what #5544 bought. Any honest
/// pre-reservation therefore implies a per-fetch ceiling below
/// `MAX_STATE_SIZE`; the only question is what it is.
///
/// A FRACTION OF THE NODE'S BUDGET, NOT A CONSTANT. This was
/// `MIN_PARKED_BYTES / (8 * MAX_DEFERRED_UPSERTS_PER_PARK)` — a `const`, so
/// a node with the full 64 MiB budget got the same allowance as one at the
/// 8 MiB floor. That is THE SAME DEFECT [`parked_budget_for`] exists to fix,
/// one level down and introduced by the same commit: the cap was made to scale
/// and its own sub-allowance was left flat, derived from the floor so it would
/// be safe there, which made it eight times too small everywhere else. It also
/// went unnoticed because the prose kept quoting the old value.
///
/// Now `budget / (4 * MAX_DEFERRED_UPSERTS_PER_PARK)`: **512 KiB at the 8 MiB
/// floor, 4 MiB at the 64 MiB ceiling**, and at either size four fully-fetching
/// parks fill the budget while the fifth degrades — the same deliberate trade
/// the park cap itself makes.
///
/// THAT DIVISOR CARRIES A SECOND DECISION, named here because a later reader
/// takes an unexplained constant for arithmetic. The previous shape allowed
/// EIGHT fetching parks per budget; this allows four. Four parks x four upserts
/// is sixteen concurrent related fetches, which is ample, and halving the
/// concurrency is part of how the per-upsert figure gets large enough to be
/// useful. It is a choice about how many parks may fetch at once, riding inside
/// a fix for how much each may retain — change it deliberately, not as a
/// by-product of retuning the allowance.
///
/// THE ALLOWANCE IS SMALL BESIDE `MAX_STATE_SIZE` (50 MiB), SO A LARGE RELATED
/// CONTRACT DEGRADES. Read those numbers together and it looks like a bug, so:
/// it is deliberate, and the alternative is a cap that does not cap — one
/// 50 MiB related state would blow the whole node-wide budget on a single park.
/// What it degrades TO is the pre-#5544 inline path: the upsert is re-run on
/// the serial loop at resume (see `contract::apply_resolved_upsert`'s caller),
/// which stalls the loop for that operation and re-fetches, but **completes the
/// write**.
///
/// AND THE INLINE PATH APPLIES NO SIZE ALLOWANCE AT ALL
/// (`contract::run_deferred_upsert_inline` -> `upsert_contract_state`), so say
/// what this design actually is rather than implying inline is bounded: **the
/// allowance bounds what a park RETAINS; exceeding it costs a stall and an
/// unbounded transient on the loop.** A 50 MiB related contract goes through
/// there unbounded. That is pre-existing — a park refused at admission already
/// went that way — but this adds a second route to it, so it is stated rather
/// than discovered. Deferring to a bound that does not exist is the same defect
/// as the transient-peak claim corrected above, and #5607 covers both paths.
///
/// EXCEEDING THE ALLOWANCE IS NOT A FAILURE, and it must not become one. An
/// over-allowance fetch used to return `Err`, which failed a write that would
/// have succeeded had the park been REFUSED instead of admitted, so whether a
/// delegate's write worked depended on how many other delegates were parked.
///
/// RESERVED AT ADMISSION (see [`task_bytes`]) and ENFORCED WHEN THE FETCH
/// COMPLETES (see `contract::within_fetch_allowance`), so retained bytes can
/// never exceed reserved bytes.
///
/// RESIDUAL, AND IT IS NOT SMALL. This bounds RETENTION — what enters the sink,
/// the resume channel and the park's lifetime, which is what this budget is
/// about. It does NOT bound the transient peak: the fetch races its sub-op GETs
/// concurrently by design, so every state is resident before the check sees the
/// total. An earlier version of this comment claimed "the transient peak stays
/// bounded by the sub-op GET path, as it already was" — **that was false and is
/// worth stating plainly rather than quietly deleting.** `start_sub_op_get`
/// takes no permit and consults no counter, there is no node-wide live-sub-op
/// cap, and inbound reassembly is an unbounded map; only the fetch timeout
/// bounds the window, which is a duration and a throughput rather than a memory
/// bound. Worst case is ~2 GiB transient for one park against the shipped
/// `MemoryMax=2G` while `parked_bytes` reads about a megabyte. Tracked in #5607;
/// bounding it belongs with the sub-op GET path, not here.
fn upsert_fetch_allowance(budget: usize) -> ByteCount {
    ByteCount::new(budget / (4 * MAX_DEFERRED_UPSERTS_PER_PARK))
}

/// Approximate heap footprint of the payloads a continuation pins.
///
/// Counts the large, contract-controlled parts — inbound states and payloads —
/// and ignores fixed-size bookkeeping. The point is to bound what an attacker
/// can grow, not to be exact.
pub(super) fn continuation_bytes(continuation: &Continuation) -> ByteCount {
    // Per ELEMENT as well as per payload — see [`ELEMENT_OVERHEAD_BYTES`]. The
    // delegate controls both list lengths, so summing payloads alone bounds
    // nothing when the payloads are empty.
    continuation
        .inbound_so_far
        .iter()
        .map(|m| ELEMENT_OVERHEAD_BYTES + inbound_bytes(m))
        .sum::<ByteCount>()
        + continuation
            .accumulated
            .iter()
            .map(|m| ELEMENT_OVERHEAD_BYTES + outbound_bytes(m))
            .sum::<ByteCount>()
        // `params` is delegate-supplied and retained for the life of the park.
        // Omitting it was one of three ways this "byte bound" failed to bound.
        + ByteCount::new(continuation.params.as_ref().len())
}

/// Approximate bytes an off-loop task retains for one park: the prompts it is
/// driving and the upserts whose related contracts it is fetching.
///
/// Charged at admission because the task holds these for exactly as long as the
/// park exists, and a single `PendingUpsert` can own a full state plus related
/// contracts plus contract code. `MAX_DEFERRED_UPSERTS_PER_PARK` caps the
/// COUNT of those, which is the same unit mismatch one level down.
pub(super) fn task_bytes(
    prompts: &[freenet_stdlib::prelude::UserInputRequest<'static>],
    upserts: &[PendingUpsert],
    // THE SAME NUMBER THE FETCH IS LATER CHECKED AGAINST, passed rather than
    // read from a constant, so the reserve and the enforcement cannot drift
    // apart. They already did once: the enforcement moved to a budget-derived
    // value and three paragraphs of prose kept quoting the old one.
    fetch_allowance: ByteCount,
) -> ByteCount {
    let prompt_bytes: ByteCount = prompts
        .iter()
        .map(|r| {
            ByteCount::new(r.message.bytes().len())
                + r.responses
                    .iter()
                    .map(|resp| ByteCount::new(resp.len()))
                    .sum::<ByteCount>()
        })
        .sum();
    let upsert_bytes: ByteCount = upserts
        .iter()
        .map(|u| {
            let update = match &u.update {
                Either::Left(state) => ByteCount::new(state.as_ref().len()),
                Either::Right(delta) => ByteCount::new(delta.as_ref().len()),
            };
            let code = u
                .code
                .as_ref()
                .map_or(ByteCount::default(), contract_container_bytes);
            // BORROW, do not clone. `clone().into_owned()` here deep-copied
            // every related state MERELY TO MEASURE IT: with up to ten 50 MiB
            // states that is hundreds of MiB allocated synchronously on the
            // serial loop, BEFORE the 64 MiB cap could reject the park —
            // causing the stall and the memory blow-up the cap exists to
            // prevent. The measurement was the harm.
            let related = related_contracts_bytes(&u.related_contracts);
            // The context is ECHOED BACK to the delegate, so it is retained for
            // the life of the park exactly as the state is, and it runs to
            // `DelegateContext::MAX_SIZE` (~400 KiB). Omitting it permitted
            // another ~100 MiB past the cap across 4 upserts x 64 parks. Same
            // omission as the two `get_context()` ones this file already
            // documents, in the one lane that does not go through a message.
            // TWICE. The task holds this `PendingUpsert` for the park's whole
            // life, and `owed_upserts` CLONES the context into the guard's
            // payload so a synthesized failure can echo it — both live until
            // the park ends. Charging once left ~1.6 MiB per park uncharged at
            // `DelegateContext::MAX_SIZE`, about 102 MiB across 64 parks
            // against a 64 MiB budget: the same hole this term was added to
            // close, reopened by the fix for a different finding in the same
            // change.
            let context = ctx_len(&u.context) + ctx_len(&u.context);
            // RESERVE BEFORE THE FETCH, not charge after it. Charging once the
            // bytes are in hand leaves dropping what you already paid to
            // retrieve as the only available response; reserving up front means
            // a node that cannot afford the fetch refuses the park instead, and
            // falls back to the inline path. See [`upsert_fetch_allowance`]
            // for why it is a fraction of the budget rather than the worst case.
            let fetch_reserve = if u.missing.is_empty() {
                ByteCount::default()
            } else {
                fetch_allowance
            };
            update + code + related + context + fetch_reserve
        })
        .sum();
    prompt_bytes + upsert_bytes
}

/// Approximate bytes a queued delegate request pins.
pub(super) fn request_bytes(req: &DelegateRequest<'static>) -> ByteCount {
    // The registration variants are NOT free: `RegisterDelegate` carries a whole
    // `DelegateContainer`, i.e. the delegate's WASM, and `DelegateRequest::key()`
    // returns that delegate's own key — so a re-registration really does queue
    // behind that delegate's park. An earlier version of this comment asserted
    // the opposite of the type definition and charged them zero, which is 8 per
    // park x 64 parks = 512 delegate modules at a counted cost of nothing.
    match req {
        DelegateRequest::ApplicationMessages {
            inbound, params, ..
        } => {
            inbound
                .iter()
                .map(|m| ELEMENT_OVERHEAD_BYTES + inbound_bytes(m))
                .sum::<ByteCount>()
                + ByteCount::new(params.as_ref().len())
        }
        DelegateRequest::RegisterDelegate { delegate, .. } => delegate_container_bytes(delegate),
        // A key and nothing else.
        DelegateRequest::UnregisterDelegate(_) => ByteCount::default(),
        // See `unmeasurable`: charged as maximal, not as free, and announced.
        other => unmeasurable("DelegateRequest", std::mem::discriminant(other)),
    }
}

// `#[allow]` on the FUNCTION: clippy emits `wildcard_enum_match_arm` at the
// match expression, so an arm-level attribute does not suppress it. Both
// `DelegateContainer` and `DelegateWasmAPIVersion` are `#[non_exhaustive]`, so
// the wildcard cannot be removed; every variant that exists is listed.
#[allow(clippy::wildcard_enum_match_arm)]
fn delegate_container_bytes(delegate: &freenet_stdlib::prelude::DelegateContainer) -> ByteCount {
    use freenet_stdlib::prelude::{DelegateContainer, DelegateWasmAPIVersion};
    // THE PARAMETERS ARE NOT FREE, and the comment this replaced said they were
    // ("the code is the large part... and is what matters for the bound"). A
    // `DelegateContainer` owns its `Parameters` as well as its WASM, and they
    // are delegate-supplied and bounded only by the ~100 MiB websocket message
    // allowance — so eight large queued re-registrations bypassed the 64 MiB
    // park budget while each was charged as a tiny module. `DelegateContainer`
    // exposes no `params()` accessor, which is presumably how this was missed;
    // the inner `Delegate` does.
    match delegate {
        DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(d)) => {
            ByteCount::new(d.code().as_ref().len()) + ByteCount::new(d.params().as_ref().len())
        }
        // UNMEASURABLE MEANS MAXIMAL HERE, NOT FREE. Both enums are
        // `#[non_exhaustive]`, so this arm cannot be removed, and a variant
        // this code cannot measure is exactly the silently-uncounted payload
        // this whole budget exists to stop — charging it 0 is the same defect
        // one variant over. Charging the whole budget makes such a request
        // refuse the park (degrading to the inline path) and refuse the queue
        // (answering the client with a rejection) rather than pass unbounded
        // bytes through a cap that reads as if it bound them. Both are loud and
        // recoverable; an uncounted bypass is neither. If you are adding a
        // variant, measure it above.
        other => unmeasurable("DelegateContainer", std::mem::discriminant(other)),
    }
}

/// Charge for a payload this build cannot measure: the whole budget, and a
/// `warn!` saying why.
///
/// THE CHARGE ALONE WOULD BE A SILENT BEHAVIOUR CHANGE. Charging maximal makes
/// an unknown variant refuse the park (degrading to the inline path) or reject
/// the queue (answering the client) instead of passing unbounded bytes through
/// a cap that reads as if it bounded them — but the first symptom of that is
/// unexplained latency, and nothing would connect it to a stdlib upgrade. So it
/// is announced, with the variant that was not recognised.
///
/// NOT A RARE PATH UNDER STDLIB-FIRST DEVELOPMENT. stdlib ships variants before
/// core learns them, so every such release makes delegates using the new
/// variant degrade until core catches up. That is the deliberate trade — a loud,
/// recoverable degradation beats a silent bypass of a memory bound — but it is a
/// known consequence rather than a surprise, and this is where a reader finds it.
fn unmeasurable(kind: &str, variant: impl std::fmt::Debug) -> ByteCount {
    // THE DISCRIMINANT, NOT THE VALUE. This used to `format!("{value:?}")`,
    // which Debug-formats the very payload the function exists because it
    // cannot afford to hold — an unmeasurable variant could carry a 50 MiB
    // state, and the log line would render all of it. The discriminant
    // identifies the variant, which is all an operator needs to match it
    // against a stdlib changelog.
    tracing::warn!(
        kind,
        ?variant,
        "Unrecognised {kind} variant in park byte accounting; charged as \
         MAXIMAL so it cannot bypass the bound. This delegate falls back to \
         the inline path (or has its request rejected) until this build learns \
         to measure the variant — expect it after a freenet-stdlib upgrade \
         that adds one"
    );
    // MAXIMAL MEANS UNADMITTABLE AT ANY BUDGET, which `MAX_PARKED_BYTES` is
    // not: admission tests `parked_bytes + bytes > budget`, so on a host whose
    // budget IS `MAX_PARKED_BYTES` the first such item satisfies `MAX > MAX`
    // as false and is admitted — the one item this charge exists to refuse.
    //
    // The type is what makes this safe to return. As a bare `usize::MAX` it
    // WRAPPED at the first `+` in whichever function composed it with a
    // sibling term, in release builds only, turning "unadmittable anywhere"
    // into "admittable almost everywhere". See [`ByteCount`].
    ByteCount::MAX
}

fn inbound_bytes(msg: &InboundDelegateMsg<'static>) -> ByteCount {
    // EXHAUSTIVE, PER VARIANT, IN THIS CRATE'S OWN MATCH.
    //
    // An earlier version routed the context charge through
    // `InboundDelegateMsg::get_context()` on the theory that a stdlib accessor
    // covering every variant made the charge impossible to forget. IT DOES NOT:
    // that accessor ends in `_ => None`, and it does not list `UserResponse` at
    // all — whose `context` is CLIENT-SUPPLIED and bounded only by
    // `DelegateContext::MAX_SIZE` (~400 KiB). So the omission moved from an arm
    // here into an arm in another crate, where it is invisible from this file
    // and no compiler error can point at it.
    //
    // The lesson is narrow and worth keeping: delegating exhaustiveness to
    // someone else's match is not a structural guarantee, it is the same hole
    // one indirection away. Only a match the compiler checks HERE, against the
    // variants this code actually retains, is one.
    match msg {
        InboundDelegateMsg::ApplicationMessage(m) => {
            ByteCount::new(m.payload.len()) + ctx_len(&m.context)
        }
        InboundDelegateMsg::GetContractResponse(r) => {
            ByteCount::new(r.state.as_ref().map_or(0, |s| s.as_ref().len())) + ctx_len(&r.context)
        }
        InboundDelegateMsg::ContractNotification(n) => {
            ByteCount::new(n.new_state.as_ref().len()) + ctx_len(&n.context)
        }
        // `response` is the client's answer bytes; `context` is separate and
        // was charged zero until #5544 H2.
        InboundDelegateMsg::UserResponse(r) => {
            ByteCount::new(r.response.len()) + ctx_len(&r.context)
        }
        InboundDelegateMsg::DelegateMessage(m) => {
            ByteCount::new(m.payload.len()) + ctx_len(&m.context)
        }
        // Small `Result` payloads, but their contexts are not small.
        InboundDelegateMsg::PutContractResponse(r) => ctx_len(&r.context),
        InboundDelegateMsg::UpdateContractResponse(r) => ctx_len(&r.context),
        InboundDelegateMsg::SubscribeContractResponse(r) => ctx_len(&r.context),
        InboundDelegateMsg::UnsubscribeContractResponse(r) => ctx_len(&r.context),
        // `tag` is bounded by stdlib's `MAX_WAKEUP_TAG_LEN`, but charge it
        // rather than assume: this arm exists precisely so nothing goes
        // uncounted. Carries no context by design.
        InboundDelegateMsg::WakeupFired { tag } => ByteCount::new(tag.len()),
        // A FOURTH `#[non_exhaustive]` SITE, and the one that matters most:
        // this is the lane `MAX_PARKED_BYTES` exists for, since
        // `inbound_so_far` holds full `WrappedState`s. It charged 0 with a
        // comment saying a new variant "MUST be added above" — an honour-system
        // requirement written as a check, which is the exact criticism this
        // change levels at the enumerated ceiling guard two files over.
        other => unmeasurable("InboundDelegateMsg", std::mem::discriminant(other)),
    }
}

fn outbound_bytes(msg: &OutboundDelegateMsg) -> ByteCount {
    // Exhaustive per variant, for the same reason as `inbound_bytes`:
    // `OutboundDelegateMsg::get_context()` also ends in `_ => None`, and the
    // wildcard swallows `ContextUpdated` — whose entire payload IS a context,
    // so routing through the accessor charged it 0 + 0. It accumulates across
    // parks via `RunSeed.accumulated` for up to MAX_CONTRACT_REQUEST_ITERATIONS
    // (#5544 H1).
    match msg {
        OutboundDelegateMsg::ApplicationMessage(m) => {
            ByteCount::new(m.payload.len()) + ctx_len(&m.context)
        }
        OutboundDelegateMsg::SendDelegateMessage(m) => {
            ByteCount::new(m.payload.len()) + ctx_len(&m.context)
        }
        OutboundDelegateMsg::ContextUpdated(c) => ctx_len(c),
        OutboundDelegateMsg::RequestUserInput(r) => {
            ByteCount::new(r.message.bytes().len())
                + r.responses
                    .iter()
                    .map(|resp| ByteCount::new(resp.len()))
                    .sum::<ByteCount>()
        }
        OutboundDelegateMsg::GetContractRequest(r) => ctx_len(&r.context),
        // `contract` and `related_contracts` were UNCOUNTED: a delegate-supplied
        // `ContractContainer` (WASM plus params) and a set of full related
        // states, both accumulating across parks via `RunSeed.accumulated`, on
        // a message whose only charged payload was `state`. Charging some of a
        // variant's fields reads more convincingly than charging none, which is
        // what let this sit under a budget that names itself a byte bound.
        OutboundDelegateMsg::PutContractRequest(r) => {
            ByteCount::new(r.state.as_ref().len())
                + contract_container_bytes(&r.contract)
                + related_contracts_bytes(&r.related_contracts)
                + ctx_len(&r.context)
        }
        // `update` was uncounted entirely — see `update_data_bytes`.
        OutboundDelegateMsg::UpdateContractRequest(r) => {
            update_data_bytes(&r.update) + ctx_len(&r.context)
        }
        OutboundDelegateMsg::SubscribeContractRequest(r) => ctx_len(&r.context),
        OutboundDelegateMsg::UnsubscribeContractRequest(r) => ctx_len(&r.context),
    }
}

/// A byte quantity in the park accounting, which SATURATES rather than wraps.
///
/// WHY A TYPE AND NOT A CAREFUL `saturating_add` AT EACH SITE. Every function
/// below composes contract- or client-supplied lengths with bare `+`, and that
/// was sound only by accident: the operands all happened to be lengths of
/// things actually allocated on the heap, so their sum could not approach
/// `usize::MAX`. That is a coincidence, not an invariant, and
/// [`unmeasurable`] introduced the first operand that is NOT a heap length.
///
/// HISTORY, NOT CURRENT BEHAVIOUR — this paragraph describes what happened
/// BEFORE this type existed, and is kept because it is the reason the type
/// exists. Do not read it as a description of the code below and conclude the
/// comment is stale; the arithmetic it describes is exactly what `ByteCount`
/// now makes unrepresentable.
///
/// One non-heap-length operand was enough to break it, and it broke in the
/// worst available direction. `overflow-checks` appears nowhere in this
/// repository — no Cargo profile, no CI, no `RUSTFLAGS`, and there is no
/// `arithmetic_side_effects` lint — so debug and test builds panicked on the
/// serial delegate loop while **release wrapped**: as bare `usize`,
/// `ELEMENT_OVERHEAD_BYTES + usize::MAX` evaluated to
/// `ELEMENT_OVERHEAD_BYTES - 1`. The sentinel that exists to make a payload
/// unadmittable at any budget instead made it admittable at almost any budget,
/// silently, in the build that ships. Both terms are `ByteCount` today, so that
/// expression saturates and the inversion is unreachable.
///
/// So the composition is fixed rather than the sentinel's route through it.
/// `unmeasurable` is not the last non-heap-length operand this will see — the
/// whole reason it exists is that new `#[non_exhaustive]` variants are
/// anticipated, and freenet-stdlib 0.10.0 shipped today.
///
/// AND THE END-TO-END CASE CANNOT BE TESTED, which is the argument that
/// settles it. Driving a real `unmeasurable` through `task_bytes` requires
/// constructing a variant this build does not know, which is impossible by
/// definition. A test can only inject the sentinel PAST the composing
/// function — as the first version of
/// `an_unmeasurable_variant_cannot_be_admitted_at_any_budget` did, passing it
/// straight to `park()` and so exercising `park()`'s own `saturating_add`
/// while never touching `task_bytes`'s internal `+`. A property that cannot be
/// tested end to end has to be structural, or it is not guaranteed at all.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub(super) struct ByteCount(usize);

impl ByteCount {
    /// Unadmittable at any budget. See [`unmeasurable`].
    pub(super) const MAX: Self = Self(usize::MAX);

    pub(super) const fn new(bytes: usize) -> Self {
        Self(bytes)
    }

    pub(super) const fn get(self) -> usize {
        self.0
    }
}

impl std::ops::Add for ByteCount {
    type Output = Self;
    fn add(self, rhs: Self) -> Self {
        Self(self.0.saturating_add(rhs.0))
    }
}

impl std::ops::Mul<usize> for ByteCount {
    type Output = Self;
    fn mul(self, rhs: usize) -> Self {
        Self(self.0.saturating_mul(rhs))
    }
}

impl std::iter::Sum for ByteCount {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self(0), |acc, n| acc + n)
    }
}

impl std::fmt::Display for ByteCount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Heap footprint of ONE element in a counted collection, beyond its payload.
///
/// FIXED TIMES UNBOUNDED IS NOT FIXED. Every function here summed payload
/// LENGTHS and charged nothing for the slot holding them, justified as
/// "fixed-size bookkeeping" — but the delegate controls the COUNT. An empty
/// `ApplicationMessage` serialises to about 21 bytes and occupies roughly 200
/// resident, and was charged zero: a 200 MiB return buffer is ~10M of them,
/// ~2.0 GB held and 0 charged, per park. `related_contracts_bytes` had the same
/// shape one level in, where a `None` entry costs a ~72-byte map slot and was
/// charged nothing.
///
/// The idiom is already in this repo: `CACHE_ENTRY_OVERHEAD_BYTES`, which
/// `declared_cache_ceiling`'s own `NOT_SUMMED` table describes as "per-entry
/// overhead CHARGED AGAINST the budgets above". Same reasoning, same fix.
/// Deliberately generous — the point is to bound what an attacker can grow, and
/// under-charging the slot is the thing that failed.
const ELEMENT_OVERHEAD_BYTES: ByteCount = ByteCount::new(256);

/// Bytes a `DelegateContext` pins. Bounded by `DelegateContext::MAX_SIZE`
/// (~400 KiB), which is why omitting it was worth two High findings.
fn ctx_len(ctx: &DelegateContext) -> ByteCount {
    ByteCount::new(ctx.as_ref().len())
}

/// Bytes a `ContractContainer` pins: the WASM AND its parameters.
///
/// Factored out because three call sites measure this same shape and two of
/// them disagreed — `task_bytes` counted code + params while
/// `delegate_container_bytes` counted code alone. One helper is how they stay
/// in step.
#[allow(clippy::wildcard_enum_match_arm)]
fn contract_container_bytes(contract: &ContractContainer) -> ByteCount {
    use freenet_stdlib::prelude::ContractWasmAPIVersion;
    // BORROW, DO NOT CLONE — THE SAME DEFECT THIS FILE ALREADY DOCUMENTS 200
    // LINES ABOVE, reintroduced by the helper that unified three call sites.
    // `ContractContainer::params()` returns `Parameters<'static>` BY VALUE
    // (a `.clone()` in stdlib's `versioning.rs`), so reading `.len()` through
    // it deep-copies the whole parameter blob. `park()` computes this and then
    // tests `over_bytes`, so a park about to be REFUSED for exceeding the
    // budget paid the full cloning cost first — measuring the thing was the
    // harm, which is exactly what `task_bytes`'s related-contract comment
    // warns about. `WrappedContract::params()` borrows.
    match contract {
        ContractContainer::Wasm(ContractWasmAPIVersion::V1(c)) => {
            ByteCount::new(c.code().data().len()) + ByteCount::new(c.params().as_ref().len())
        }
        other => unmeasurable("ContractContainer", std::mem::discriminant(other)),
    }
}

/// Bytes the states carried inside a `RelatedContracts` pin.
fn related_contracts_bytes(related: &RelatedContracts<'static>) -> ByteCount {
    related
        .states()
        .map(|(_, st)| {
            // The map slot costs whether or not the state is present, and the
            // delegate chooses how many entries there are.
            ELEMENT_OVERHEAD_BYTES + ByteCount::new(st.as_ref().map_or(0, |s| s.as_ref().len()))
        })
        .sum()
}

/// Bytes an `UpdateData` pins — a full state, a delta, or both.
///
/// UNCOUNTED BEFORE THIS. `outbound_bytes` charged an
/// `OutboundDelegateMsg::UpdateContractRequest` its context and nothing else,
/// while `update` carries a state bounded only by `MAX_STATE_SIZE` (50 MiB) and
/// accumulates across parks through `RunSeed.accumulated`. Same class as the
/// two `get_context()` omissions this file already documents, and found the
/// same way: by asking what each field of each variant actually retains rather
/// than what the variant is called.
//
// The `#[allow]` sits on the FUNCTION, not on the arm: clippy emits
// `wildcard_enum_match_arm` at the match EXPRESSION, so an arm-level attribute
// does not suppress it — the same trap #5554 hit, where a local `cargo test`
// was green while CI's `-D warnings` went red. The wildcard is required because
// `UpdateData` is `#[non_exhaustive]`; every variant that exists IS listed.
#[allow(clippy::wildcard_enum_match_arm)]
fn update_data_bytes(update: &UpdateData<'static>) -> ByteCount {
    match update {
        UpdateData::State(state) => ByteCount::new(state.as_ref().len()),
        UpdateData::Delta(delta) => ByteCount::new(delta.as_ref().len()),
        UpdateData::StateAndDelta { state, delta } => {
            ByteCount::new(state.as_ref().len()) + ByteCount::new(delta.as_ref().len())
        }
        UpdateData::RelatedState { state, .. } => ByteCount::new(state.as_ref().len()),
        UpdateData::RelatedDelta { delta, .. } => ByteCount::new(delta.as_ref().len()),
        UpdateData::RelatedStateAndDelta { state, delta, .. } => {
            ByteCount::new(state.as_ref().len()) + ByteCount::new(delta.as_ref().len())
        }
        // See `unmeasurable` for why this is the whole budget rather than
        // nothing, and why it is announced.
        other => unmeasurable("UpdateData", std::mem::discriminant(other)),
    }
}

/// Backstop lifetime for a park.
///
/// The [`ParkGuard`] already guarantees exactly-one resume per park even if
/// the spawned task is dropped, panics or is cancelled, and both parkable
/// waits are internally bounded (`USER_INPUT_TIMEOUT` = 60 s for a prompt,
/// `DEFERRED_RELATED_FETCH_TIMEOUT` = `OPERATION_TTL` + 2 s for a related
/// fetch). This TTL is the third layer: it covers a task that neither
/// completes nor drops, which no current path can produce but which a future
/// one could. On expiry the park is force-resumed — pending drained, responder
/// answered — so a wedged delegate can never be wedged forever.
///
/// Set above each inner budget INDIVIDUALLY so the real timeout normally wins
/// and this rarely fires first on a merely-slow operation; a park cut short at
/// its own TTL reports a spurious failure for work that was about to succeed.
///
/// NOT above their SUM, which an earlier version implied. `run_user_input_prompts`
/// awaits prompts SEQUENTIALLY, so two prompts can sum to 120 s against a 90 s
/// TTL — this fires first, and the delegate is resumed with whatever answers
/// had arrived. `PARK_WORK_BUDGET` (75 s) caps the whole task body, which is
/// what actually bounds the sequence; the ordering below is what keeps that cap
/// the one that bites.
///
/// NOT ON THE SIMULATED CLOCK, and this is why the backstop's own coverage is
/// thin. `parked_at` is a `tokio::time::Instant` and the loop waits on
/// `tokio::time::sleep_until`, while a simulated node runs on
/// `crate::simulation::VirtualTime` — a separate clock that does not advance
/// either. So advancing simulation time past this TTL does NOT expire a park,
/// and whether the backstop fires there depends on real scheduler time.
/// Direct time access in `crates/core` is disallowed by
/// `.claude/rules/testing.md` for exactly this reason.
///
/// **The TTL backstop is therefore untestable in simulation today** (#5605).
/// Stated rather than left for the next person to rediscover, because it
/// explains an absence: the sweep's tests all drive `DelegateParkCtx` directly
/// with a caller-supplied `now`, and none drives it through a simulated node.
///
/// Closing it means threading the node's `TimeSource` through the registry and
/// the deadline wait, and it is not a local change: no `TimeSource` exists
/// anywhere in the contract layer or at any of `contract_handling`'s call
/// sites. It also cannot stop at this constant — `PARK_WORK_BUDGET`'s
/// `tokio::time::timeout` and `DEFERRED_RELATED_FETCH_TIMEOUT` would remain on
/// the tokio clock, leaving the park on two clocks and the
/// `PARK_WORK_BUDGET < PARK_TTL` ordering below comparing unlike things. That
/// ordering is load-bearing (see [`PARK_WORK_BUDGET`]), so a partial conversion
/// would be worse than none.
pub(super) const PARK_TTL: Duration = Duration::from_secs(90);

/// Cap on the off-loop task's own runtime, kept BELOW [`PARK_TTL`].
///
/// The task runs this iteration's prompts and related-contract fetches
/// concurrently, but several prompts (each up to `USER_INPUT_TIMEOUT`) could
/// still sum past the TTL. If that happened the loop's backstop sweep would
/// force-resume the park while the task was still working, and the task's own
/// result would then arrive for a park that no longer exists and be discarded.
/// Bounding the task below the TTL gives the guard a 15 s MARGIN in that race.
/// It is a margin, not a guarantee, and the difference matters: `parked_at` is
/// stamped in [`DelegateParkCtx::park`] BEFORE the task is spawned, while the
/// task's own `timeout(PARK_WORK_BUDGET, ..)` starts at its first poll. The
/// two clocks are separated by however long the runtime takes to schedule the
/// task, so what is checked below is arithmetic on nominal durations, not an
/// ordering the scheduler is obliged to honour.
///
/// An earlier version of this said "the guard always wins that race". That is
/// the same shape of over-strong claim whose sibling ("the `ParkGuard` always
/// resumes the park first") is what hid #5554, so it is stated as a margin
/// here. The residual it leaves is real but narrow: `run_user_input_prompts`
/// pushes each answer into the shared sink as it arrives, while the guard does
/// not `send()` until the whole body finishes, so between those two instants a
/// human's answer exists and the sweep — which reads the CHANNEL, never the
/// sink — cannot see it. If the task is starved past the margin the sweep
/// force-resumes and that answer is discarded. Closing it properly is the same
/// close [`DelegateParkCtx::should_force_resume`] already names: let the
/// registry own a slot the guard writes synchronously.
pub(super) const PARK_WORK_BUDGET: Duration = Duration::from_secs(75);

/// The budget/TTL ordering above is load-bearing, so it is CHECKED rather than
/// merely described. Tune one of these and the compiler makes you tune the
/// other — prose in two rustdoc blocks is exactly the kind of coupling that
/// rots the first time someone adjusts a timeout in isolation.
const _: () = assert!(
    PARK_WORK_BUDGET.as_secs() < PARK_TTL.as_secs(),
    "PARK_WORK_BUDGET must stay below PARK_TTL: the off-loop task has to \
     finish and deliver its resume before the loop's backstop sweep would \
     force-resume the park, or the task's result is discarded"
);

/// A delegate invocation that arrived while its delegate was parked.
///
/// Held here rather than requeued into the fair queue: every delegate request
/// shares the single `QueueKey::Default` lane (`fair_queue.rs`), so a
/// pop-see-parked-repush cycle would busy-spin the loop.
///
/// Two variants because the two entry points differ in how their result is
/// delivered, and a queued run must resume through the SAME path it would have
/// taken had it not been queued. Collapsing them would route a notification's
/// residual messages to a client responder that does not exist.
pub(super) enum PendingRun {
    /// Client-driven (`ContractHandlerEvent::DelegateRequest`). Answers `id`.
    Client {
        id: EventId,
        req: DelegateRequest<'static>,
        origin_contract: Option<ContractInstanceId>,
        connection_scope: ConnectionScope,
        user_context: Option<UserSecretContext>,
    },
    /// Contract-notification-driven. No client; residual `ApplicationMessage`s
    /// fan out to the apps registered with the delegate.
    ///
    /// Queued rather than dropped, but note the delivered state may be STALE by
    /// the time it drains — up to `PARK_TTL`. That is within the notification
    /// pipeline's documented contract, which is explicitly best-effort and
    /// lossy (`send_delegate_contract_notifications`: "Delegates that require
    /// guaranteed delivery should poll contract state periodically"). Running
    /// it immediately is NOT an option: it would clobber the parked
    /// continuation's context, which is the whole reason for the exclusion.
    ///
    /// COALESCED per contract rather than capped, and not counted against
    /// [`MAX_PENDING_PER_DELEGATE`]. Rejecting the 9th notification would be a
    /// silent loss landing on exactly the wrong population: ghostkeys parks on
    /// prompts, so the rejection window is precisely when a user is
    /// interacting, and Harvest with many address contracts subscribed would
    /// lose payment notifications there. The window is reachable in practice —
    /// Harvest's bridge backfill replays thirty blocks on restart and can emit
    /// several claims for one script within seconds.
    ///
    /// # PRECONDITION: the contract's state must be ACCUMULATING
    ///
    /// Newest-wins is lossless only if the newest state SUBSUMES what a
    /// superseded notification carried. That is a property of the CONTRACT, not
    /// of this mechanism, and the node cannot tell the two apart:
    ///
    /// - **Holds** for a grow-only or CRDT-merge state. Harvest's `ClaimSetV1`
    ///   is a `BTreeMap` merged by set union, deliberately grow-only because a
    ///   Bitcoin reorg must be expressible without deletion — a retraction is a
    ///   NEWER assertion at a higher height, not an edit. Coalescing is lossless
    ///   there.
    /// - **Does NOT hold** for a register-valued contract, where each update
    ///   REPLACES the previous. Payment A sets `state = A`, payment B sets
    ///   `state = B`; dropping A's notification means the delegate never learns
    ///   A happened. Such a contract needs every distinct notification kept,
    ///   not collapsed.
    ///
    /// The register shape is the more natural modelling, and Harvest said so
    /// themselves — grow-only was a deliberate, slightly unusual choice. So this
    /// is an assumption the notification API currently makes ON THE DELEGATE'S
    /// BEHALF, and nothing here enforces it. #5467 is the place to decide
    /// whether a delegate should be able to say "do not coalesce mine", or a
    /// contract should declare its shape.
    ///
    /// One bound worth re-checking if `PARK_TTL` ever grows: grow-only is itself
    /// capped (Harvest prunes at `MAX_CLAIMS` = 512, lowest-`as_of` first), so
    /// "newest contains everything" holds only until that budget binds. Anything
    /// arriving inside the current park window is by definition newest and is
    /// not what gets pruned, so it does not affect this today.
    Notification {
        /// The contract whose change triggered this. Carried explicitly so
        /// queued notifications can be COALESCED per contract.
        contract_id: ContractInstanceId,
        req: DelegateRequest<'static>,
    },
}

/// Where a resumed run's residual `ApplicationMessage`s must go.
///
/// The two entry points differ: a client-driven run answers the parked client
/// responder (so the client sees ONE response covering the whole round-trip,
/// exactly as it does today when the loop blocks), while a
/// notification-driven run has no client and fans out to the apps registered
/// with the delegate — the same route `handle_delegate_notification` already
/// uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Delivery {
    Client,
    Apps,
}

/// Everything needed to re-enter a parked delegate on a later loop iteration.
///
/// `params` is carried explicitly and is NOT optional: `DelegateKey` identity
/// covers `BLAKE3(code_hash ‖ params)` and the params are threaded into the
/// WASM env, so resuming with empty params (as the notification path does, a
/// known v1 limitation) would run a different delegate instance.
pub(super) struct Continuation {
    /// Iterations this round-trip has already consumed, so
    /// `MAX_CONTRACT_REQUEST_ITERATIONS` bounds the WHOLE round-trip rather
    /// than each leg of it (#5544 S1).
    ///
    /// Without this the counter is a call-frame local that every park resets,
    /// so a delegate emitting `RequestUserInput` on every re-entry loops
    /// park -> resume -> park forever, holding its exclusion open the whole
    /// time and rejecting every other request for it. Before parking existed
    /// the same delegate stopped after 100 iterations.
    ///
    /// Scope boundary: this covers PARKS, not contract NOTIFICATIONS. A
    /// notification is a genuinely new invocation and resets the count, which
    /// is correct — and is also why #5558 (a delegate notified of its own
    /// writes) is a separate unbounded loop that this does not close.
    pub iterations: usize,
    pub params: Parameters<'static>,
    pub origin_contract: Option<ContractInstanceId>,
    pub connection_scope: ConnectionScope,
    pub user_context: Option<UserSecretContext>,
    pub inter_delegate: super::InterDelegateDispatch,
    /// Outbound messages the delegate produced before it parked. Carried so the
    /// client sees ONE response covering the whole round-trip rather than a
    /// partial one now and the rest out-of-band.
    pub accumulated: Vec<OutboundDelegateMsg>,
    /// Responses already computed for this iteration, awaiting the parked one.
    pub inbound_so_far: Vec<InboundDelegateMsg<'static>>,
    /// The parked client's responder, if this run descends from a client
    /// request. Attached by the caller immediately after parking (it owns the
    /// channel the responder is taken from), and re-attached by the resume
    /// handler if the resumed run parks again — a delegate that prompts twice
    /// in a row must not strand its client.
    pub responder: Option<StashedResponder>,
    pub delivery: Delivery,
}

/// One parked delegate.
struct ParkEntry {
    continuation: Continuation,
    /// Identity of THIS park; see [`DelegateResume::epoch`].
    epoch: u64,
    parked_at: tokio::time::Instant,
    /// Bytes retained by the OFF-LOOP TASK for this park — the prompts and the
    /// deferred upserts it is holding. Not part of the continuation, but
    /// retained for exactly as long, and each `PendingUpsert` can own a full
    /// state plus related contracts and code. Charged so the byte cap bounds
    /// what is actually held rather than only what this struct points at.
    task_bytes: usize,
    /// Client requests, FIFO, capped by [`MAX_PENDING_PER_DELEGATE`]. Rejection
    /// is acceptable here precisely because the caller can be TOLD.
    pending_clients: VecDeque<PendingRun>,
    /// Newest pending notification per contract. Superseded ones are dropped,
    /// which loses nothing the successor does not carry.
    pending_notifications: HashMap<ContractInstanceId, DelegateRequest<'static>>,
    /// ARRIVAL ORDER of the contracts in `pending_notifications`.
    ///
    /// The map alone would drain in hash order. Each drained notification runs
    /// delegate WASM and can mutate secrets and contracts, so hash order makes
    /// observable effects reorder between runs and identical simulation runs
    /// diverge — the determinism hazard `testing.md` names. `expired()` was
    /// sorted for the same reason (L7); this is the same defect one function
    /// over, on the path that actually executes delegate code.
    ///
    /// Coalescing keeps a contract's ORIGINAL position: a newer notification
    /// replaces the value, not the slot, so ordering stays arrival order.
    notification_order: VecDeque<ContractInstanceId>,
    /// Bytes held by the queued items above, so they count toward
    /// [`MAX_PARKED_BYTES`] like the continuation does. Without this the
    /// coalescing map would be a fresh count-bounded-but-not-byte-bounded hole
    /// of exactly the kind #5551 tracks: 64 contracts of 50 MB state is 3.2 GB.
    pending_bytes: usize,
}

/// Why a park ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResumeCause {
    /// The awaited work delivered a result.
    Completed,
    /// The park outlived [`PARK_TTL`] (see its rustdoc — a backstop, not an
    /// expected path).
    TimedOut,
}

/// A delegate PUT/UPDATE that could not complete because the contract asked for
/// related contracts this node does not hold.
///
/// The fetch is off-loaded (it is a network GET, and awaiting it on the loop is
/// the second #5544 stall); the upsert itself must be RE-RUN on the loop,
/// because it runs WASM and WASM stays serial. So the park carries everything
/// needed to re-run it.
pub(super) struct PendingUpsert {
    /// Identity for THIS upsert, distinct even from another naming the same
    /// contract with the same `is_put`.
    ///
    /// Reconciliation used to match on `(contract, is_put)` as a multiset,
    /// which is exact for COUNTING but not for PAIRING: two deferred upserts
    /// sharing that key whose fetches finish out of order let the later one
    /// consume the earlier's obligation, so the delegate got a success and a
    /// failure carrying the WRONG contexts and no response for one request.
    /// A counter makes the pairing exact and the multiset bookkeeping
    /// unnecessary.
    pub id: UpsertId,
    pub key: ContractKey,
    pub update: Either<WrappedState, StateDelta<'static>>,
    pub related_contracts: RelatedContracts<'static>,
    pub code: Option<ContractContainer>,
    /// `true` builds a `PutContractResponse`, `false` an
    /// `UpdateContractResponse`.
    pub is_put: bool,
    /// Echoed back to the delegate so it can match the response to its request.
    pub context: DelegateContext,
    /// The related contracts to fetch off-loop.
    pub missing: Vec<ContractInstanceId>,
}

/// Identity of one deferred upsert. Monotonic per process; only equality
/// matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct UpsertId(u64);

impl UpsertId {
    pub(super) fn next() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        Self(NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed))
    }
}

/// One upsert a park owes a response for, carried so a SYNTHESIZED failure can
/// echo what the delegate sent.
///
/// Was `(ContractInstanceId, bool)`. The context was dropped on the floor and
/// `handle_delegate_resume` rebuilt the failure with `DelegateContext::default()`
/// — its comment said "the `PendingUpsert` that carried it is gone by then",
/// which was true and was the bug rather than the reason. Delegates use the
/// echoed context to correlate a response with the request that produced it, so
/// a defaulted one can be applied to the WRONG logical request, and is simply
/// unusable when two requests target the same contract. Reachable on every
/// abnormal exit: panic, cancellation, or the off-loop budget expiring.
///
/// THE IDENTITY IS `id`, NOT `(contract, is_put)`. This paragraph said the
/// opposite until a reviewer read it against the code twelve lines below:
/// reconciliation is a `HashSet<UpsertId>` match, and `contract` and `is_put`
/// are carried for the synthesized failure message rather than as the key.
/// A multiset match on `(contract, is_put)` is precisely what `UpsertId`
/// replaced, because a later completion cancelled an earlier obligation for
/// the same contract. Leftover prose from the draft before that change.
pub(super) struct OwedUpsert {
    /// Matched against `ResolvedUpsert::pending.id` — see [`PendingUpsert::id`].
    pub id: UpsertId,
    pub contract: ContractInstanceId,
    pub is_put: bool,
    pub context: DelegateContext,
}

/// What a park OWES, derived from the upserts its off-loop task is carrying.
///
/// A FUNCTION rather than an inline `map` at the call site, and the reason is
/// the defect it fixes. The context was dropped where this list was built, in
/// `handle_delegate_with_contract_requests`, and the guard-level test that
/// exercised the synthesized failure could not see it: that test builds a
/// `ParkGuard` directly, so it pinned that the guard PRESERVES what it is
/// given, never that the caller gives it the right thing. Falsifying the fix
/// against the old inline construction proved that — defaulting the context at
/// the call site left the whole suite green.
///
/// With the construction here there is no context expression at the call site
/// to get wrong, and `owed_upserts_carry_the_delegate_s_context` covers the one
/// place it can be.
pub(super) fn owed_upserts(upserts: &[PendingUpsert]) -> Vec<OwedUpsert> {
    upserts
        .iter()
        .map(|u| OwedUpsert {
            id: u.id,
            contract: *u.key.id(),
            is_put: u.is_put,
            // ECHOED BACK on a synthesized failure. See `OwedUpsert::context`.
            context: u.context.clone(),
        })
        .collect()
}

/// What an off-loop related fetch produced, and what the loop must do with it.
///
/// THREE STATES, NOT A `Result` PLUS A FLAG. The third case — the fetch
/// succeeded but retained more than the park reserved — is neither a success
/// nor a failure, and encoding it as `Err` is what made an over-allowance fetch
/// FAIL A WRITE that would have succeeded had the park been refused instead of
/// admitted. A delegate's write then depended on how many OTHER delegates were
/// parked, which inverts the signal a pressure indicator is supposed to give.
pub(super) enum FetchDisposition {
    /// Within the reserve. Use these states (or report this failure).
    Resolved(Result<Vec<(ContractInstanceId, WrappedState)>, ExecutorError>),
    /// Over the reserve. The states have been DROPPED — nothing oversized ever
    /// reaches the sink, the guard or the resume channel — and the upsert must
    /// be re-run INLINE on the serial loop at resume, which is the degradation
    /// [`upsert_fetch_allowance`] documents: slower, holds the loop, re-fetches,
    /// and completes the write.
    RetryInline,
}

/// A [`PendingUpsert`] whose off-loop fetch has finished, one way or the other.
pub(super) struct ResolvedUpsert {
    pub pending: PendingUpsert,
    pub fetched: FetchDisposition,
}

/// Sent from an off-loop task back to the `contract_handling` loop.
pub(super) struct DelegateResume {
    pub delegate_key: DelegateKey,
    /// Which PARK this resume belongs to (#5544 H1).
    ///
    /// A park is identified by `(key, epoch)`, not by key alone. The TTL
    /// backstop ends a park by force-resuming it WITHOUT consuming the off-loop
    /// task's `ParkGuard`, so that guard still owes a resume. If the delegate
    /// has re-parked by the time it arrives, matching on key alone would hand
    /// the OLD continuation's messages to the NEW park — the cross-round-trip
    /// context corruption this whole mechanism exists to prevent, reached
    /// through the backstop itself. The epoch makes the stale resume
    /// identifiable and droppable.
    pub epoch: u64,
    pub cause: ResumeCause,
    /// Messages that are ready to feed straight back into the delegate (the
    /// prompt path). Empty on a dropped or timed-out park, which still resumes
    /// so the continuation terminates.
    pub inbound: Vec<InboundDelegateMsg<'static>>,
    /// Upserts whose related contracts were fetched off-loop and which must be
    /// RE-RUN on the loop before their responses can be built.
    pub upserts: Vec<ResolvedUpsert>,
    /// Upserts the off-loop task never resolved — it panicked, was cancelled,
    /// or ran out of budget. `(contract, is_put)`, turned into failure
    /// responses by the resume handler so the delegate is told.
    pub unresolved_upserts: Vec<OwedUpsert>,
}

/// RAII guard guaranteeing an off-loop task delivers EXACTLY ONE
/// [`DelegateResume`] for its park — on success, or on drop / panic /
/// cancellation before it got there.
///
/// Same load-bearing invariant as #4391's `ResumeGuard`: because every park is
/// answered exactly once, the loop needs no stale-resume guard, a parked client
/// responder can never be stranded, and a delegate's pending queue is always
/// drained. Never zero (Drop covers early exit), never twice (the success path
/// takes the payload, so Drop sees `None`).
///
/// The resume channel is unbounded, so both sends are non-blocking. Producers
/// are bounded by [`MAX_PARKED_DELEGATES`], the receiver is the loop (which
/// drains every iteration), and the task never reads what the loop produces —
/// no cycle, per `channel-safety.md`'s carve-out.
pub(super) struct ParkGuard {
    payload: Option<ParkGuardPayload>,
}

struct ParkGuardPayload {
    resume_tx: tokio::sync::mpsc::UnboundedSender<DelegateResume>,
    delegate_key: DelegateKey,
    epoch: u64,
    /// Prompt request ids this park owes a `UserResponse` for. A MULTISET:
    /// `request_id` is chosen by delegate WASM, so `[7, 7]` is reachable.
    owed_prompts: Vec<u32>,
    /// Upserts this park owes a response for, keyed by [`UpsertId`].
    ///
    /// `deferred_upserts` is built by two independent loops (PUTs and UPDATEs)
    /// with no de-duplication, so two entries for the same contract are
    /// reachable and were originally reconciled as a MULTISET of
    /// `(contract, is_put)`. That is what `UpsertId` replaced: under the
    /// multiset match a later completion cancelled an EARLIER obligation for
    /// the same contract, so one of the two was answered twice and the other
    /// never. This doc described the multiset for one commit longer than the
    /// code did.
    owed_upserts: Vec<OwedUpsert>,
    /// Where the off-loop task deposits results AS THEY COMPLETE. Shared with
    /// the task rather than created inside it, so `Drop` can see work that
    /// finished before a panic or cancellation (#5544 F2).
    answers: std::sync::Arc<std::sync::Mutex<Vec<InboundDelegateMsg<'static>>>>,
    fetches: std::sync::Arc<std::sync::Mutex<Vec<ResolvedUpsert>>>,
}

impl ParkGuard {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        resume_tx: tokio::sync::mpsc::UnboundedSender<DelegateResume>,
        delegate_key: DelegateKey,
        epoch: u64,
        owed_prompts: Vec<u32>,
        owed_upserts: Vec<OwedUpsert>,
        answers: std::sync::Arc<std::sync::Mutex<Vec<InboundDelegateMsg<'static>>>>,
        fetches: std::sync::Arc<std::sync::Mutex<Vec<ResolvedUpsert>>>,
    ) -> Self {
        Self {
            payload: Some(ParkGuardPayload {
                resume_tx,
                delegate_key,
                epoch,
                owed_prompts,
                owed_upserts,
                answers,
                fetches,
            }),
        }
    }

    /// Deliver whatever the task completed. Consumes the payload so a later
    /// `Drop` is a no-op (exactly-once).
    ///
    /// Takes NO results: they are read from the shared sinks, so this path and
    /// `Drop` see the same data by construction. An earlier version passed them
    /// in, which is why `Drop` saw none (#5544 F2).
    pub(super) fn send(mut self) {
        if let Some(p) = self.payload.take() {
            Self::deliver(p, ResumeCause::Completed);
        }
    }

    fn deliver(p: ParkGuardPayload, cause: ResumeCause) {
        let ParkGuardPayload {
            resume_tx,
            delegate_key,
            epoch,
            owed_prompts,
            owed_upserts,
            answers,
            fetches,
        } = p;

        // POISON-TOLERANT, and that is the whole point of this guard. It runs
        // from `Drop`, which is reached when the off-loop task PANICS — and the
        // task panics while holding one of these very locks whenever it dies
        // inside `run_user_input_prompts`' `sink.lock().unwrap().push(..)` or
        // the fetch closure's. `lock().unwrap()` on a poisoned mutex panics,
        // and a panic in `Drop` during unwinding ABORTS THE PROCESS. Recovering
        // the inner value costs nothing and is correct here: the data behind
        // the lock is a `Vec` that is only ever pushed to, so a writer that
        // died mid-push left it consistent, and delivering whatever it holds is
        // exactly what this path exists to do.
        let mut inbound = std::mem::take(&mut *answers.lock().unwrap_or_else(|e| e.into_inner()));
        let upserts = std::mem::take(&mut *fetches.lock().unwrap_or_else(|e| e.into_inner()));

        // TERMINAL RESULTS ARE PRODUCED HERE, not in the task body, so that
        // EVERY exit produces them — including a panic or a cancellation, which
        // reach `Drop` and never run the task's own cleanup.
        //
        // IF YOU ARE ADDING AN EXIT PATH, RE-ASK THE QUESTION HERE. "Answered on
        // every exit" is not a property you establish once for a change; it has
        // to be re-asked at EVERY level that has exits. That is not
        // hypothetical: the same change, in the same session, put an RAII guard
        // on the prompt REGISTRY entry — correct "on every exit" reasoning —
        // and then put the response synthesis one level up in the task body,
        // where a panic never reaches it.
        //
        // The reason to make this structural rather than to rely on noticing is
        // NOT that people are careless. It is that THE BOUNDARY WHERE THE
        // QUESTION NEEDS RE-ASKING IS INVISIBLE FROM EITHER SIDE OF IT. Nothing
        // at this `Drop` impl announces "you are now at a different level of
        // the same question", and nothing at the task body announced it either.
        //
        // RECONCILED BY COUNT, NOT BY SET (#5544 F1/F3). Both owed lists are
        // multisets — `request_id` is delegate-chosen, and two upserts can name
        // one contract — so filtering by membership let ONE completion cancel
        // the obligation for BOTH, and the delegate waited forever for a
        // response nothing remained to produce. That is reachable on the
        // ordinary budget-expiry path too, not just on panic, because partial
        // results are delivered by design.
        let mut answered: HashMap<u32, usize> = HashMap::new();
        for msg in &inbound {
            if let InboundDelegateMsg::UserResponse(r) = msg {
                *answered.entry(r.request_id).or_default() += 1;
            }
        }
        for request_id in owed_prompts {
            match answered.get_mut(&request_id) {
                Some(n) if *n > 0 => *n -= 1,
                _ => inbound.push(InboundDelegateMsg::UserResponse(
                    freenet_stdlib::prelude::UserInputResponse {
                        request_id,
                        response: freenet_stdlib::prelude::ClientResponse::new(Vec::new()),
                        context: DelegateContext::default(),
                    },
                )),
            }
        }

        // BY IDENTITY, not by (contract, is_put) multiset. The multiset was
        // exact for counting and wrong for PAIRING: two upserts sharing a
        // contract and direction, finishing out of order, let the later
        // completion cancel the earlier's obligation — so the delegate received
        // a success and a synthesized failure carrying each other's contexts,
        // and one request got no response at all. `id` is unique per upsert, so
        // an obligation can only be discharged by its own completion.
        let resolved: std::collections::HashSet<UpsertId> =
            upserts.iter().map(|r| r.pending.id).collect();
        let unresolved_upserts: Vec<OwedUpsert> = owed_upserts
            .into_iter()
            .filter(|owed| !resolved.contains(&owed.id))
            .collect();
        if !unresolved_upserts.is_empty() {
            tracing::warn!(
                delegate = %delegate_key,
                count = unresolved_upserts.len(),
                "Off-loop delegate work ended without resolving every upsert; \
                 synthesizing failures so the delegate is told rather than left \
                 waiting (#5544)"
            );
        }

        if resume_tx
            .send(DelegateResume {
                delegate_key: delegate_key.clone(),
                epoch,
                cause,
                inbound,
                upserts,
                unresolved_upserts,
            })
            .is_err()
        {
            tracing::debug!(
                delegate = %delegate_key,
                "Delegate resume channel closed; contract-handling loop gone"
            );
        }
    }
}

impl Drop for ParkGuard {
    fn drop(&mut self) {
        if let Some(p) = self.payload.take() {
            tracing::warn!(
                delegate = %p.delegate_key,
                "Off-loop delegate task dropped before sending — delivering an \
                 empty resume so the park terminates, its pending queue drains \
                 and the parked client is answered exactly once (#5544)"
            );
            Self::deliver(p, ResumeCause::TimedOut);
        }
    }
}

/// Outcome of asking to park a delegate.
pub(super) enum ParkAdmission {
    /// Parked. The caller must spawn the off-loop work with a [`ParkGuard`]
    /// carrying this `epoch`, so a stale resume can be told from a live one.
    Admitted { epoch: u64 },
    /// Refused (node-wide cap). The caller keeps the old inline behaviour;
    /// the continuation is handed back so nothing is lost.
    Refused(Box<Continuation>),
}

/// Outcome of offering a request for an already-parked delegate.
pub(super) enum QueueOutcome {
    /// Queued behind the park; it will run when the delegate resumes.
    Queued,
    /// The delegate's queue is full. The caller must answer this request with a
    /// visible error rather than dropping it.
    Rejected(Box<PendingRun>),
}

/// Loop-owned per-delegate park state.
pub(super) struct DelegateParkCtx {
    parked: HashMap<DelegateKey, ParkEntry>,
    /// Running total of every retained payload across live parks (#5544 S4):
    /// continuations, off-loop task work, and queued pending runs.
    parked_bytes: usize,
    /// What `parked_bytes` is measured against, from [`parked_budget_for`].
    budget: usize,
    /// The host RAM `budget` was scaled from, carried only so a refusal can say
    /// where its limit came from.
    host_ram: usize,
    /// Source of park identities; see [`DelegateResume::epoch`].
    next_epoch: u64,
    /// Refusal counters, per cause (L9). A refusal that is only logged is a
    /// clean zero to anything reading metrics — the same pattern this branch
    /// fixed for the over-cap client request.
    refused: RefusalCounts,
    /// Handed to each [`ParkGuard`] so an off-loop task can deliver its resume.
    /// Kept here rather than threaded separately so every call site needs only
    /// a single `&mut DelegateParkCtx`.
    resume_tx: tokio::sync::mpsc::UnboundedSender<DelegateResume>,
}

/// Why parked work was turned away, counted rather than only logged (L9).
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RefusalCounts {
    /// Parks refused at the node-wide count or byte cap.
    pub parks: u64,
    /// Client requests refused because the pending queue was full or over budget.
    pub client_requests: u64,
    /// Notifications dropped at the distinct-contract cap or the byte budget.
    pub notifications: u64,
}

impl DelegateParkCtx {
    pub(super) fn new(resume_tx: tokio::sync::mpsc::UnboundedSender<DelegateResume>) -> Self {
        // Read once, at construction: the budget is a property of the host, and
        // re-reading it per admission would make the cap wobble under memory
        // pressure exactly when it most needs to be stable.
        let host_ram =
            crate::wasm_runtime::read_total_ram_bytes().unwrap_or(PARKED_FALLBACK_TOTAL_RAM_BYTES);
        let budget = parked_budget_for(host_ram);
        Self {
            parked: HashMap::new(),
            parked_bytes: 0,
            budget,
            host_ram,
            next_epoch: 0,
            refused: RefusalCounts::default(),
            resume_tx,
        }
    }

    /// The byte budget this registry is enforcing. Tests assert against THIS
    /// rather than against `MAX_PARKED_BYTES`, so they stay true on a host
    /// whose scaled budget is smaller than the clamp ceiling.
    #[cfg(test)]
    pub(super) fn budget(&self) -> usize {
        self.budget
    }

    /// The bytes ONE deferred upsert's off-loop related fetch may retain on
    /// this node. See [`upsert_fetch_allowance`].
    pub(super) fn upsert_fetch_allowance(&self) -> ByteCount {
        upsert_fetch_allowance(self.budget)
    }

    pub(super) fn resume_tx(&self) -> &tokio::sync::mpsc::UnboundedSender<DelegateResume> {
        &self.resume_tx
    }

    /// `true` if this delegate currently has a parked continuation, and so must
    /// not be re-entered by a fresh request.
    pub(super) fn is_parked(&self, key: &DelegateKey) -> bool {
        self.parked.contains_key(key)
    }

    /// Snapshot of what has been turned away, by cause.
    ///
    /// The running totals also ride on each refusal's own `warn!`/`info!`, so
    /// production observability does not depend on anything calling this; this
    /// accessor is what lets a test pin that the counting happens at all.
    #[cfg(test)]
    pub(super) fn refusals(&self) -> RefusalCounts {
        self.refused
    }

    /// The live epoch for `key`, for tests that need to end a park they did not
    /// capture the epoch from. Deliberately test-only: production code always
    /// has the epoch from `ParkAdmission::Admitted` or the resume itself, and a
    /// helper that looked one up by key would defeat the identity check.
    #[cfg(test)]
    pub(super) fn epoch_of(&self, key: &DelegateKey) -> Option<u64> {
        self.parked.get(key).map(|e| e.epoch)
    }

    #[cfg(test)]
    pub(super) fn parked_count(&self) -> usize {
        self.parked.len()
    }

    /// Park `key`, or refuse at the node-wide cap.
    ///
    /// A delegate that is already parked cannot park again — the exclusion
    /// guarantees only one round-trip per delegate is ever in flight, so this
    /// is unreachable by construction and is treated as a refusal rather than
    /// silently clobbering the live continuation.
    pub(super) fn park(
        &mut self,
        key: DelegateKey,
        continuation: Continuation,
        // Bytes the off-loop task will retain for this park (see [`task_bytes`]).
        task_bytes: ByteCount,
    ) -> ParkAdmission {
        // THE ONE CONVERSION, and it is the accumulator boundary. Everything
        // upstream composes in [`ByteCount`], which saturates; `parked_bytes`
        // is a plain `usize` running total whose own add already saturates.
        // Keeping the newtype all the way to here is what makes a wrapping `+`
        // on contract-supplied lengths impossible rather than merely absent.
        let bytes = (continuation_bytes(&continuation) + task_bytes).get();
        let over_bytes = self.parked_bytes.saturating_add(bytes) > self.budget;
        if self.parked.len() >= MAX_PARKED_DELEGATES || self.parked.contains_key(&key) || over_bytes
        {
            tracing::warn!(
                delegate = %key,
                parked = self.parked.len(),
                limit = MAX_PARKED_DELEGATES,
                parked_bytes = self.parked_bytes,
                adding_bytes = bytes,
                // The BUDGET and the RAM it was scaled from, together. On a
                // small host `parked_budget_for` gives less than the 64 MiB
                // ceiling, so parks are refused sooner and fall back inline
                // more often — #5544's original problem returning at small
                // sizes. That is the deliberate trade for an aggregate that
                // actually holds, and an operator seeing more inline fallback
                // on a small VPS has to be able to find out why from one line.
                byte_limit = self.budget,
                host_ram = self.host_ram,
                over_bytes,
                already_parked = self.parked.contains_key(&key),
                total_refused_parks = self.refused.parks.saturating_add(1),
                "Refusing to park delegate; falling back to the inline path"
            );
            self.refused.parks = self.refused.parks.saturating_add(1);
            return ParkAdmission::Refused(Box::new(continuation));
        }
        self.parked_bytes = self.parked_bytes.saturating_add(bytes);
        let epoch = self.next_epoch;
        self.next_epoch = self.next_epoch.wrapping_add(1);
        self.parked.insert(
            key,
            ParkEntry {
                continuation,
                epoch,
                task_bytes: task_bytes.get(),
                parked_at: tokio::time::Instant::now(),
                pending_clients: VecDeque::new(),
                pending_notifications: HashMap::new(),
                notification_order: VecDeque::new(),
                pending_bytes: 0,
            },
        );
        ParkAdmission::Admitted { epoch }
    }

    /// Queue a request that arrived for a parked delegate.
    ///
    /// Caller must have checked [`is_parked`](Self::is_parked); queueing for an
    /// unparked delegate is a caller bug and is reported back as a rejection so
    /// the request is still answered.
    pub(super) fn queue_pending(&mut self, key: &DelegateKey, req: PendingRun) -> QueueOutcome {
        let parked_bytes = self.parked_bytes;
        let budget = self.budget;
        let Some(entry) = self.parked.get_mut(key) else {
            return QueueOutcome::Rejected(Box::new(req));
        };

        match req {
            // COALESCE, do not reject. A superseded notification carries
            // nothing its successor does not, so replacing is lossless in a way
            // rejecting is not — and rejecting would land on ghostkeys and
            // Harvest exactly when they are most active.
            PendingRun::Notification { contract_id, req } => {
                let bytes = request_bytes(&req).get();
                let superseded = entry
                    .pending_notifications
                    .get(&contract_id)
                    .map_or(ByteCount::default(), request_bytes)
                    .get();
                // `contains_key` alone is the question. An earlier
                // `superseded == 0 &&` conjunct was dead weight that also read
                // as if a zero-byte entry were no entry (L11).
                let is_new_contract = !entry.pending_notifications.contains_key(&contract_id);

                if is_new_contract
                    && entry.pending_notifications.len() >= MAX_PENDING_NOTIFICATION_CONTRACTS
                {
                    tracing::info!(
                        delegate = %key,
                        contract = %contract_id,
                        limit = MAX_PENDING_NOTIFICATION_CONTRACTS,
                        total_dropped = self.refused.notifications.saturating_add(1),
                        "Dropped a notification: too many distinct contracts already \
                         queued behind this park"
                    );
                    self.refused.notifications = self.refused.notifications.saturating_add(1);
                    return QueueOutcome::Rejected(Box::new(PendingRun::Notification {
                        contract_id,
                        req,
                    }));
                }

                let projected = parked_bytes
                    .saturating_add(bytes)
                    .saturating_sub(superseded);
                if projected > budget {
                    tracing::info!(
                        delegate = %key,
                        contract = %contract_id,
                        parked_bytes,
                        adding_bytes = bytes,
                        byte_limit = budget,
                        total_dropped = self.refused.notifications.saturating_add(1),
                        "Dropped a notification: queueing it would exceed the parked \
                         byte budget"
                    );
                    self.refused.notifications = self.refused.notifications.saturating_add(1);
                    return QueueOutcome::Rejected(Box::new(PendingRun::Notification {
                        contract_id,
                        req,
                    }));
                }

                entry.pending_bytes = entry.pending_bytes.saturating_add(bytes);
                if is_new_contract {
                    entry.notification_order.push_back(contract_id);
                }
                if let Some(old) = entry.pending_notifications.insert(contract_id, req) {
                    let freed = request_bytes(&old).get();
                    entry.pending_bytes = entry.pending_bytes.saturating_sub(freed);
                    self.parked_bytes = self.parked_bytes.saturating_sub(freed);
                    tracing::debug!(
                        delegate = %key,
                        contract = %contract_id,
                        "Coalesced a superseded notification behind a park"
                    );
                }
                self.parked_bytes = self.parked_bytes.saturating_add(bytes);
                QueueOutcome::Queued
            }
            // Client requests keep the cap: over it, the caller is TOLD.
            client => {
                if entry.pending_clients.len() >= MAX_PENDING_PER_DELEGATE {
                    tracing::warn!(
                        delegate = %key,
                        queued = entry.pending_clients.len(),
                        limit = MAX_PENDING_PER_DELEGATE,
                        total_refused = self.refused.client_requests.saturating_add(1),
                        "Delegate pending queue full while parked — rejecting request"
                    );
                    self.refused.client_requests = self.refused.client_requests.saturating_add(1);
                    return QueueOutcome::Rejected(Box::new(client));
                }
                let bytes = match &client {
                    PendingRun::Client { req, .. } => request_bytes(req).get(),
                    PendingRun::Notification { .. } => 0,
                };
                // Check the PROJECTED total BEFORE inserting. Adding first and
                // checking never was worse than an overshoot: once
                // `parked_bytes` passed the cap, `park()` refused EVERY delegate
                // node-wide and everything fell back to inline stalls, so one
                // local app pushing large ApplicationMessages behind a single
                // park could disable parking for the whole node — reinstating
                // the exact stall this change removes.
                if parked_bytes.saturating_add(bytes) > budget {
                    tracing::warn!(
                        delegate = %key,
                        parked_bytes,
                        adding_bytes = bytes,
                        byte_limit = budget,
                        total_refused = self.refused.client_requests.saturating_add(1),
                        "Refusing to queue a delegate request: it would exceed the \
                         parked byte budget"
                    );
                    self.refused.client_requests = self.refused.client_requests.saturating_add(1);
                    return QueueOutcome::Rejected(Box::new(client));
                }
                entry.pending_bytes = entry.pending_bytes.saturating_add(bytes);
                self.parked_bytes = self.parked_bytes.saturating_add(bytes);
                entry.pending_clients.push_back(client);
                QueueOutcome::Queued
            }
        }
    }

    /// Hand the parked client's responder to a live park.
    ///
    /// Separate from [`park`](Self::park) because the responder is taken from
    /// the contract-handler channel, which the caller owns and this registry
    /// deliberately knows nothing about. A `None` responder (client already
    /// gone) is stored as-is: the park still has to terminate.
    pub(super) fn attach_responder(
        &mut self,
        key: &DelegateKey,
        responder: Option<StashedResponder>,
    ) {
        match self.parked.get_mut(key) {
            Some(entry) => entry.continuation.responder = responder,
            None => {
                // Unreachable: the caller attaches immediately after a
                // successful park, on the same loop iteration, and nothing
                // else can end a park in between. Log rather than panic — a
                // dropped response is recoverable, a panicked loop is not.
                tracing::error!(
                    delegate = %key,
                    "attach_responder for a delegate that is not parked; the \
                     client for this run will not be answered"
                );
            }
        }
    }

    /// End the park identified by `(key, epoch)`, returning its continuation
    /// and everything queued behind it.
    ///
    /// Returns `None` when the epoch does not match — a STALE resume, from an
    /// off-loop task whose park was already ended by the TTL backstop and whose
    /// delegate has since re-parked. Matching on key alone would feed the old
    /// continuation's messages into the new park (#5544 H1).
    /// ORDERING NOTE: `parked_bytes` is decremented here, while the memory is
    /// freed when the CALLER drops the continuation it is handed. Between the
    /// two, admission sees capacity that is not yet physically free. Deliberate
    /// and in the safe direction for the thing this guards — a park is ending,
    /// so the bytes are going away — but it does mean `parked_bytes` is a
    /// bound on what is COMMITTED, not a reading of resident memory. See #5607
    /// for the larger version of that distinction on the fetch path.
    pub(super) fn take_matching(
        &mut self,
        key: &DelegateKey,
        epoch: u64,
    ) -> Option<(Continuation, VecDeque<PendingRun>)> {
        match self.parked.get(key) {
            Some(entry) if entry.epoch == epoch => {}
            Some(entry) => {
                tracing::warn!(
                    delegate = %key,
                    stale_epoch = epoch,
                    live_epoch = entry.epoch,
                    "Dropping a STALE park resume: this delegate re-parked after \
                     its previous park was force-resumed by the TTL backstop. \
                     Absorbing it would feed the old continuation's messages to \
                     the new park (#5544 H1)"
                );
                return None;
            }
            None => return None,
        }
        self.parked.remove(key).map(|entry| {
            self.parked_bytes = self
                .parked_bytes
                .saturating_sub(continuation_bytes(&entry.continuation).get())
                .saturating_sub(entry.task_bytes)
                .saturating_sub(entry.pending_bytes);
            // Client requests first, then coalesced notifications. Clients have
            // a caller waiting on a response; notifications do not, and their
            // ordering is already approximate because coalescing drops
            // superseded ones.
            let mut pending: VecDeque<PendingRun> = entry.pending_clients;
            // Drain notifications in ARRIVAL order, not hash order. Each one
            // runs delegate WASM, so hash order would make observable effects
            // reorder between runs.
            let mut notifications = entry.pending_notifications;
            pending.extend(
                entry
                    .notification_order
                    .into_iter()
                    .filter_map(|contract_id| {
                        notifications
                            .remove(&contract_id)
                            .map(|req| PendingRun::Notification { contract_id, req })
                    }),
            );
            debug_assert!(
                notifications.is_empty(),
                "every coalesced notification must have an arrival-order slot"
            );
            (entry.continuation, pending)
        })
    }

    /// The earliest instant at which some park will reach [`PARK_TTL`], or
    /// `None` when nothing is parked.
    ///
    /// The loop uses this to arm a timer in its idle `select!`. Without it the
    /// backstop sweep only runs when some UNRELATED event happens to wake the
    /// loop, so on a quiet node — the normal state for a background peer, and
    /// exactly the condition under which a prompt goes unanswered because no
    /// dashboard tab is open — a wedged park would never be swept. A backstop
    /// whose firing depends on other traffic is not a backstop.
    pub(super) fn next_sweep_deadline(&self) -> Option<tokio::time::Instant> {
        self.parked
            .values()
            .map(|entry| entry.parked_at + PARK_TTL)
            .min()
    }

    /// Keys whose park has outlived [`PARK_TTL`] AND whose result is not
    /// already in the loop's hands.
    ///
    /// Returned rather than acted on so the caller (which owns the executor and
    /// the channel) performs the force-resume; this keeps the registry a pure
    /// data structure and unit-testable without a loop.
    ///
    /// `already_delivered` is the loop's buffer of resumes it has taken off
    /// `delegate_resume_rx` but not yet run. **A park listed there must not be
    /// swept**, and this is the load-bearing half of the signature. (Every
    /// "#5554" below is the PR that added parking, where this was found in
    /// review, not a typo for the #5544 issue the rest of this file cites.)
    /// The sweep ends a park WITHOUT consuming the off-loop task's
    /// [`ParkGuard`], so a resume that arrives afterwards is rejected by
    /// [`Self::take_matching`] on epoch and dropped — including everything it
    /// carries. That payload is `deliver()`'s output, which is where a human's
    /// answer lives: force-resuming a park whose guard has ALREADY delivered
    /// throws away the `UserResponse` the user gave and re-enters the delegate
    /// with `inbound: Vec::new()`, so it is told nothing about the prompt it
    /// asked — not even a denial. The backstop exists for a park that produced
    /// NOTHING; one that produced an answer is not wedged, it is queued, and it
    /// runs on the next iteration.
    ///
    /// Matching is by `(key, epoch)`, not key alone: a buffered resume from an
    /// EARLIER park of the same delegate (one the backstop already swept) is
    /// stale, carries nothing the live park is owed, and must not shield it.
    ///
    /// # Why this takes the RECEIVER and not just the buffer
    ///
    /// The first version of this fix took `&VecDeque` and left the caller to
    /// drain the channel into it. That is not enough, and the reason is the
    /// whole bug: **the loop AWAITS between draining and sweeping.** It runs a
    /// batch of resumes first, and a `ParkGuard` firing during that await puts
    /// its resume in the CHANNEL, which a buffer snapshotted beforehand cannot
    /// see. The sweep then force-resumed a park whose answer had already
    /// arrived — bit-for-bit the bug this was supposed to close, on a window
    /// that reaches `USER_INPUT_TIMEOUT` (60 s) whenever the park table is full
    /// and a resume falls through to the inline prompt wait. That is precisely
    /// the condition that makes resumes queue in the first place, so the
    /// failure concentrated where it was most likely.
    ///
    /// Taking the receiver makes the snapshot and the decision ONE synchronous
    /// step, so no caller can ask this question from a stale view — the
    /// ordering is enforced by the signature rather than by a comment or a
    /// source pin. (A pin cannot express it: `drain` textually preceding
    /// `expired` is exactly what the buggy code did. Position cannot express
    /// duration.)
    pub(super) fn expired(
        &self,
        now: tokio::time::Instant,
        resume_rx: &mut tokio::sync::mpsc::UnboundedReceiver<DelegateResume>,
        already_delivered: &mut VecDeque<DelegateResume>,
    ) -> Vec<(DelegateKey, u64)> {
        absorb_delivered(resume_rx, already_delivered);
        let mut out: Vec<(DelegateKey, u64)> = self
            .parked
            .iter()
            .filter(|(_, entry)| now.duration_since(entry.parked_at) >= PARK_TTL)
            .filter(|(key, entry)| !resume_in_hand(already_delivered, key, entry.epoch))
            .map(|(key, entry)| (key.clone(), entry.epoch))
            .collect();
        // Deterministic order: `HashMap` iteration is arbitrary, and a sweep
        // that force-resumes several parks should not do so in a different
        // order run to run (L7).
        out.sort_by_key(|(_, epoch)| *epoch);
        out
    }

    /// Re-ask, for ONE park, the question [`Self::expired`] answered for the
    /// batch: may the backstop still force-resume it?
    ///
    /// The sweep loop AWAITS — force-resuming park X re-enters WASM — so the
    /// list `expired` returned is a decision made before that await, and a
    /// guard firing while X is being resumed is invisible to the decision
    /// already made about Y. Same defect as the outer one, one scope in, and
    /// it needs the same remedy rather than an argument about how short the
    /// window is.
    ///
    /// Call this immediately before each force-resume, with no `.await`
    /// between: it and [`Self::take_matching`] (the first statement of
    /// `handle_delegate_resume`) are both synchronous, so the observation and
    /// the removal cannot be separated by a suspension point.
    ///
    /// RESIDUAL, stated rather than implied: a guard that fires in the
    /// instants between this call and `take_matching` is still lost. That is
    /// inherent to a lock-free channel plus a sweep that does not consume the
    /// guard, and closing it would mean the registry owning a slot the guard
    /// writes synchronously. What changed is the size of the hole: from a
    /// window bounded by a 60-second human wait to one bounded by two adjacent
    /// synchronous statements on the same task.
    ///
    /// **Absence of an await is not absence of a window.** "No `.await`
    /// between" is exactly the sentence a later reader turns into
    /// "impossible", and it is not: it bounds how long THIS task spends
    /// between looking and removing, and says nothing about the other side.
    /// `ParkGuard::deliver` runs on other worker threads and may `send()` at
    /// any instant, including this one. The claim here is about duration, not
    /// impossibility.
    pub(super) fn should_force_resume(
        &self,
        key: &DelegateKey,
        epoch: u64,
        resume_rx: &mut tokio::sync::mpsc::UnboundedReceiver<DelegateResume>,
        already_delivered: &mut VecDeque<DelegateResume>,
    ) -> bool {
        absorb_delivered(resume_rx, already_delivered);
        !resume_in_hand(already_delivered, key, epoch)
    }
}

/// Move every resume the off-loop tasks have sent into the loop's buffer.
///
/// Synchronous by construction — `try_recv` never yields — which is the
/// property the callers depend on: a drain that could suspend would reopen the
/// window it exists to close.
fn absorb_delivered(
    resume_rx: &mut tokio::sync::mpsc::UnboundedReceiver<DelegateResume>,
    already_delivered: &mut VecDeque<DelegateResume>,
) {
    while let Ok(resume) = resume_rx.try_recv() {
        already_delivered.push_back(resume);
    }
}

/// Whether `already_delivered` holds the resume for exactly this park.
///
/// **The epoch half is what does the work; the key half is redundant today.**
/// `next_epoch` is ONE counter per registry ([`DelegateParkCtx::park`]
/// increments it on every admission, not per delegate), so no two live parks
/// can share an epoch and matching on epoch alone would already be exact. A
/// mutation that drops the key comparison therefore survives every test, and
/// that is a true fact about the code rather than a coverage gap — worth
/// stating, because the pair reads as jointly load-bearing and a future reader
/// would otherwise go looking for the test that pins it.
///
/// It is kept for two reasons. It is the assertion that makes the intent local
/// — "this resume belongs to THIS park" — rather than something a reader has to
/// go and confirm by finding the counter. And it is what stops the redundancy
/// becoming a bug if `next_epoch` is ever made per-delegate, which is an
/// entirely reasonable future change that would silently make epoch-only
/// matching collide across delegates.
///
/// The direction that IS pinned, by
/// `a_stale_buffered_resume_does_not_shield_the_current_park`, is the opposite
/// one: matching on KEY alone is wrong, because a resume from an earlier park
/// of the same delegate would shield the current one and disarm the backstop
/// for exactly the delegate that has already needed it.
fn resume_in_hand(
    already_delivered: &VecDeque<DelegateResume>,
    key: &DelegateKey,
    epoch: u64,
) -> bool {
    already_delivered
        .iter()
        .any(|resume| resume.epoch == epoch && &resume.delegate_key == key)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------------------------
    // `usize` shims for the byte functions, shadowing the `super::*` imports.
    //
    // Assertions compare against plain integers and do not ACCUMULATE, so the
    // saturating discipline `ByteCount` exists to enforce buys nothing here and
    // would cost a `ByteCount::new` around every literal. Production code has
    // no such shim: `park()` is the single conversion point, and these are
    // deliberately confined to `mod tests` so they cannot be reached from it.
    // ---------------------------------------------------------------------
    fn continuation_bytes(c: &Continuation) -> usize {
        super::continuation_bytes(c).get()
    }
    fn task_bytes(
        prompts: &[freenet_stdlib::prelude::UserInputRequest<'static>],
        upserts: &[PendingUpsert],
        fetch_allowance: usize,
    ) -> usize {
        super::task_bytes(prompts, upserts, ByteCount::new(fetch_allowance)).get()
    }
    fn request_bytes(req: &DelegateRequest<'static>) -> usize {
        super::request_bytes(req).get()
    }
    fn inbound_bytes(msg: &InboundDelegateMsg<'static>) -> usize {
        super::inbound_bytes(msg).get()
    }
    fn outbound_bytes(msg: &OutboundDelegateMsg) -> usize {
        super::outbound_bytes(msg).get()
    }
    fn upsert_fetch_allowance(budget: usize) -> usize {
        super::upsert_fetch_allowance(budget).get()
    }
    fn unmeasurable(kind: &str, variant: impl std::fmt::Debug) -> usize {
        super::unmeasurable(kind, variant).get()
    }

    // =====================================================================
    // Byte accounting: EVERY term, individually falsifiable.
    // =====================================================================
    //
    // A mutation campaign zeroed the `SendDelegateMessage` and
    // `PutContractRequest` payload terms of `outbound_bytes` and the whole
    // 5486-test suite stayed green. Seven of the nine byte-accounting variants
    // were never exercised at all, and the two that were, survived.
    //
    // That is worse than the omissions codex found, because it is the reason
    // they could exist: a corrected `task_bytes` that nothing can falsify is
    // the same defect one layer up.
    //
    // AND THE FIRST VERSION OF THIS COMMENT OVER-CLAIMED IN EXACTLY THAT WAY.
    // It said every term here was individually falsifiable while the tests
    // below covered `outbound_bytes` and `inbound_bytes` only — so the two
    // terms I had just ADDED to `task_bytes` were pinned and its five
    // pre-existing ones were not, including a 50 MiB-capable state and the
    // whole `ApplicationMessages` arm that every ordinary delegate call takes.
    // Nine of twelve terms could be zeroed with the full suite green. I pinned
    // what I fixed and wrote a sentence about the general case, which is the
    // same shape as the context test that asserted a true property of the wrong
    // object — both written in one change, by someone looking for exactly this.
    //
    // So each of these gives every payload-bearing term a DISTINCT size and
    // asserts the sum, and `task_bytes` and `request_bytes` now have the same
    // treatment as the two message functions. Zero any single term
    // and the total drops below it — no term can stand in for another, and no
    // case passes because a sibling term happened to be large.
    //
    // The tests that existed did the opposite: they set the payload EMPTY and
    // asserted only that the context was charged, which is exactly why the
    // payload terms could be deleted without anything noticing.

    const T_PAYLOAD: usize = 8 * 1024;
    const T_CTX: usize = 4 * 1024;
    const T_STATE: usize = 16 * 1024;
    const T_CODE: usize = 2 * 1024;
    const T_PARAMS: usize = 1024;
    const T_RELATED: usize = 32 * 1024;
    const T_DELTA: usize = 512;
    /// A fetch allowance for `task_bytes` tests, distinct from every other size
    /// here so it cannot be confused with a payload term.
    const TEST_ALLOWANCE: usize = 64 * 1024;

    fn t_ctx() -> DelegateContext {
        DelegateContext::new(vec![0u8; T_CTX])
    }

    fn t_contract() -> ContractContainer {
        use freenet_stdlib::prelude::{ContractCode, ContractWasmAPIVersion, WrappedContract};
        ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
            std::sync::Arc::new(ContractCode::from(vec![1u8; T_CODE])),
            Parameters::from(vec![2u8; T_PARAMS]),
        )))
    }

    fn t_related() -> RelatedContracts<'static> {
        RelatedContracts::from(HashMap::from([(
            ContractInstanceId::new([9u8; 32]),
            Some(freenet_stdlib::prelude::State::from(vec![3u8; T_RELATED])),
        )]))
    }

    fn t_prompt() -> freenet_stdlib::prelude::UserInputRequest<'static> {
        let message = freenet_stdlib::prelude::NotificationMessage::try_from(
            &serde_json::Value::String("m".repeat(T_PAYLOAD)),
        )
        .expect("notification message");
        freenet_stdlib::prelude::UserInputRequest {
            request_id: 1,
            message,
            responses: vec![freenet_stdlib::prelude::ClientResponse::new(vec![
                4u8;
                T_STATE
            ])],
        }
    }

    /// Every `OutboundDelegateMsg` variant charges every payload it retains.
    ///
    /// These accumulate across parks through `RunSeed.accumulated` for up to
    /// `MAX_CONTRACT_REQUEST_ITERATIONS`, so an uncounted field here is
    /// multiplied before it is ever noticed.
    ///
    /// FALSIFY by zeroing any single term in `outbound_bytes` — including the
    /// two the mutation campaign zeroed with the suite staying green, and the
    /// `PutContractRequest::contract` / `related_contracts` /
    /// `UpdateContractRequest::update` terms that were never charged at all.
    #[test]
    fn every_outbound_variant_charges_every_payload_it_retains() {
        let cases: Vec<(&str, OutboundDelegateMsg, usize)> = vec![
            (
                "ApplicationMessage",
                OutboundDelegateMsg::ApplicationMessage(
                    freenet_stdlib::prelude::ApplicationMessage::new(vec![0u8; T_PAYLOAD])
                        .with_context(t_ctx()),
                ),
                T_PAYLOAD + T_CTX,
            ),
            (
                "SendDelegateMessage",
                OutboundDelegateMsg::SendDelegateMessage(
                    freenet_stdlib::prelude::DelegateMessage {
                        target: key(1),
                        sender: key(2),
                        payload: vec![0u8; T_PAYLOAD],
                        context: t_ctx(),
                        processed: false,
                    },
                ),
                T_PAYLOAD + T_CTX,
            ),
            (
                "ContextUpdated",
                OutboundDelegateMsg::ContextUpdated(t_ctx()),
                T_CTX,
            ),
            (
                "RequestUserInput",
                OutboundDelegateMsg::RequestUserInput(t_prompt()),
                T_PAYLOAD + T_STATE,
            ),
            (
                "GetContractRequest",
                OutboundDelegateMsg::GetContractRequest(
                    freenet_stdlib::prelude::GetContractRequest {
                        contract_id: ContractInstanceId::new([5u8; 32]),
                        context: t_ctx(),
                        processed: false,
                    },
                ),
                T_CTX,
            ),
            (
                "PutContractRequest",
                OutboundDelegateMsg::PutContractRequest(
                    freenet_stdlib::prelude::PutContractRequest {
                        contract: t_contract(),
                        state: WrappedState::new(vec![0u8; T_STATE]),
                        related_contracts: t_related(),
                        context: t_ctx(),
                        processed: false,
                    },
                ),
                T_STATE + T_CODE + T_PARAMS + T_RELATED + T_CTX,
            ),
            (
                "UpdateContractRequest",
                OutboundDelegateMsg::UpdateContractRequest(
                    freenet_stdlib::prelude::UpdateContractRequest {
                        contract_id: ContractInstanceId::new([5u8; 32]),
                        update: UpdateData::StateAndDelta {
                            state: freenet_stdlib::prelude::State::from(vec![0u8; T_STATE]),
                            delta: StateDelta::from(vec![0u8; T_DELTA]),
                        },
                        context: t_ctx(),
                        processed: false,
                    },
                ),
                T_STATE + T_DELTA + T_CTX,
            ),
            (
                "SubscribeContractRequest",
                OutboundDelegateMsg::SubscribeContractRequest(
                    freenet_stdlib::prelude::SubscribeContractRequest {
                        contract_id: ContractInstanceId::new([5u8; 32]),
                        context: t_ctx(),
                        processed: false,
                    },
                ),
                T_CTX,
            ),
        ];
        for (name, msg, expected) in cases {
            let charged = outbound_bytes(&msg);
            assert!(
                charged >= expected,
                "{name}: charged {charged}, but it retains at least {expected} \
                 bytes. Every payload-bearing field of this variant must be \
                 counted — an uncounted one is retained for the life of the \
                 park under a cap that reads as if it bounded it"
            );
        }
    }

    /// The same, for every `InboundDelegateMsg` variant.
    ///
    /// FALSIFY by zeroing any single term in `inbound_bytes`.
    #[test]
    fn every_inbound_variant_charges_every_payload_it_retains() {
        let cid = ContractInstanceId::new([5u8; 32]);
        let cases: Vec<(&str, InboundDelegateMsg<'static>, usize)> = vec![
            (
                "ApplicationMessage",
                InboundDelegateMsg::ApplicationMessage(
                    freenet_stdlib::prelude::ApplicationMessage::new(vec![0u8; T_PAYLOAD])
                        .with_context(t_ctx()),
                ),
                T_PAYLOAD + T_CTX,
            ),
            (
                "GetContractResponse",
                InboundDelegateMsg::GetContractResponse(
                    freenet_stdlib::prelude::GetContractResponse {
                        contract_id: cid,
                        state: Some(WrappedState::new(vec![0u8; T_STATE])),
                        context: t_ctx(),
                    },
                ),
                T_STATE + T_CTX,
            ),
            (
                "ContractNotification",
                InboundDelegateMsg::ContractNotification(
                    freenet_stdlib::prelude::ContractNotification {
                        contract_id: cid,
                        new_state: WrappedState::new(vec![0u8; T_STATE]),
                        context: t_ctx(),
                    },
                ),
                T_STATE + T_CTX,
            ),
            (
                "UserResponse",
                InboundDelegateMsg::UserResponse(freenet_stdlib::prelude::UserInputResponse {
                    request_id: 1,
                    response: freenet_stdlib::prelude::ClientResponse::new(vec![0u8; T_PAYLOAD]),
                    context: t_ctx(),
                }),
                T_PAYLOAD + T_CTX,
            ),
            (
                "DelegateMessage",
                InboundDelegateMsg::DelegateMessage(freenet_stdlib::prelude::DelegateMessage {
                    target: key(1),
                    sender: key(2),
                    payload: vec![0u8; T_PAYLOAD],
                    context: t_ctx(),
                    processed: false,
                }),
                T_PAYLOAD + T_CTX,
            ),
            (
                "PutContractResponse",
                InboundDelegateMsg::PutContractResponse(
                    freenet_stdlib::prelude::PutContractResponse {
                        contract_id: cid,
                        result: Ok(()),
                        context: t_ctx(),
                    },
                ),
                T_CTX,
            ),
            (
                "UpdateContractResponse",
                InboundDelegateMsg::UpdateContractResponse(
                    freenet_stdlib::prelude::UpdateContractResponse {
                        contract_id: cid,
                        result: Ok(()),
                        context: t_ctx(),
                    },
                ),
                T_CTX,
            ),
            (
                "SubscribeContractResponse",
                InboundDelegateMsg::SubscribeContractResponse(
                    freenet_stdlib::prelude::SubscribeContractResponse {
                        contract_id: cid,
                        result: Ok(()),
                        context: t_ctx(),
                    },
                ),
                T_CTX,
            ),
        ];
        for (name, msg, expected) in cases {
            let charged = inbound_bytes(&msg);
            assert!(
                charged >= expected,
                "{name}: charged {charged}, but it retains at least {expected} bytes"
            );
        }
    }

    /// Every term of `task_bytes`, individually.
    ///
    /// The five pre-existing terms — prompt message, prompt responses, the
    /// upsert's update, its contract code+params, its related states — were
    /// unfalsifiable until now; only the two this change added were pinned.
    /// Each gets a distinct size and is asserted as a DELTA against a baseline
    /// that has it empty, so no term can be satisfied by a sibling.
    ///
    /// FALSIFY by zeroing any single term in `task_bytes`.
    #[test]
    fn every_task_bytes_term_is_charged() {
        fn upsert(
            update: Either<WrappedState, StateDelta<'static>>,
            code: Option<ContractContainer>,
            related: RelatedContracts<'static>,
            context: DelegateContext,
            missing: Vec<ContractInstanceId>,
        ) -> PendingUpsert {
            PendingUpsert {
                id: UpsertId::next(),
                key: ContractKey::from_params_and_code(
                    Parameters::from(vec![]),
                    freenet_stdlib::prelude::ContractCode::from(vec![0u8; 4]),
                ),
                update,
                related_contracts: related,
                code,
                is_put: false,
                context,
                missing,
            }
        }
        let empty = || {
            upsert(
                Either::Right(StateDelta::from(Vec::new())),
                None,
                RelatedContracts::default(),
                DelegateContext::default(),
                Vec::new(),
            )
        };
        let base = task_bytes(&[], &[empty()], TEST_ALLOWANCE);

        // The prompt lane: message and responses are separate terms.
        let prompts = [t_prompt()];
        assert!(
            task_bytes(&prompts, &[empty()], TEST_ALLOWANCE) >= base + T_PAYLOAD + T_STATE,
            "a prompt's message AND its responses must both be charged"
        );

        let cases: Vec<(&str, PendingUpsert, usize)> = vec![
            (
                "update state",
                upsert(
                    Either::Left(WrappedState::new(vec![0u8; T_STATE])),
                    None,
                    RelatedContracts::default(),
                    DelegateContext::default(),
                    Vec::new(),
                ),
                T_STATE,
            ),
            (
                "update delta",
                upsert(
                    Either::Right(StateDelta::from(vec![0u8; T_DELTA])),
                    None,
                    RelatedContracts::default(),
                    DelegateContext::default(),
                    Vec::new(),
                ),
                T_DELTA,
            ),
            (
                "contract code and params",
                upsert(
                    Either::Right(StateDelta::from(Vec::new())),
                    Some(t_contract()),
                    RelatedContracts::default(),
                    DelegateContext::default(),
                    Vec::new(),
                ),
                T_CODE + T_PARAMS,
            ),
            (
                "related states",
                upsert(
                    Either::Right(StateDelta::from(Vec::new())),
                    None,
                    t_related(),
                    DelegateContext::default(),
                    Vec::new(),
                ),
                T_RELATED,
            ),
            (
                // TWICE, and asserting only `T_CTX` did not catch charging it
                // once — found by falsifying this test rather than by reading
                // it. The task holds the `PendingUpsert` and `owed_upserts`
                // clones the context into the guard, so both live for the
                // park's life; a single charge leaves half of it uncounted.
                "echoed context, charged for BOTH copies",
                upsert(
                    Either::Right(StateDelta::from(Vec::new())),
                    None,
                    RelatedContracts::default(),
                    t_ctx(),
                    Vec::new(),
                ),
                2 * T_CTX,
            ),
            (
                "fetch reserve",
                upsert(
                    Either::Right(StateDelta::from(Vec::new())),
                    None,
                    RelatedContracts::default(),
                    DelegateContext::default(),
                    vec![ContractInstanceId::new([1u8; 32])],
                ),
                TEST_ALLOWANCE,
            ),
        ];
        for (name, u, expected) in cases {
            let charged = task_bytes(&[], &[u], TEST_ALLOWANCE);
            assert!(
                charged >= base + expected,
                "task_bytes: the `{name}` term must be charged — {charged} \
                 against a {base} baseline, expected at least {expected} more"
            );
        }
    }

    /// H6: the per-ELEMENT charge, which no payload assertion can see.
    ///
    /// Every other test here gives its payloads a size, so a missing
    /// per-element term is invisible to all of them — zeroing
    /// `ELEMENT_OVERHEAD_BYTES` left the whole suite green. The delegate
    /// controls the COUNT, so empty messages are the attack: ~10M of them fit
    /// in a 200 MiB return buffer at ~200 resident bytes each, and were charged
    /// nothing.
    ///
    /// FALSIFY by setting `ELEMENT_OVERHEAD_BYTES` to 0.
    #[test]
    fn empty_messages_are_charged_for_their_slots() {
        // ASSERT GROWTH, NOT A PRODUCT OF THE CONSTANT UNDER TEST. The first
        // version asserted `charged >= N * ELEMENT_OVERHEAD_BYTES`, which is
        // SELF-REFERENTIAL: zeroing the constant zeroes the expectation too, so
        // `charged >= 0` held and the test passed under the exact mutation it
        // is named for. The campaign caught it; reading it did not, twice.
        //
        // Growth cannot be defeated that way. With the constant at 0, N empty
        // messages and one empty message both cost nothing, so the difference
        // is 0 and this goes red.
        const N: usize = 512;
        let charge_for = |count: usize| {
            let mut cont = continuation();
            cont.inbound_so_far = (0..count)
                .map(|_| {
                    InboundDelegateMsg::ApplicationMessage(
                        freenet_stdlib::prelude::ApplicationMessage::new(Vec::new()),
                    )
                })
                .collect();
            continuation_bytes(&cont)
        };
        let one = charge_for(1);
        let many = charge_for(N);
        assert!(
            many > one,
            "{N} EMPTY messages must cost more than one. They carry no payload, \
             so only a per-element charge distinguishes them — and the delegate \
             picks the count: ~10M of them fit a 200 MiB buffer. Charged {many} \
             for {N} against {one} for 1"
        );
        assert!(
            many - one >= N - 1,
            "each additional empty message must cost at least a byte; got \
             {} across {} extra messages",
            many - one,
            N - 1
        );

        let charge_out = |count: usize| {
            let mut cont = continuation();
            cont.accumulated = (0..count)
                .map(|_| {
                    OutboundDelegateMsg::ApplicationMessage(
                        freenet_stdlib::prelude::ApplicationMessage::new(Vec::new()),
                    )
                })
                .collect();
            continuation_bytes(&cont)
        };
        assert!(
            charge_out(N) > charge_out(1),
            "the same holds for the accumulated lane, which grows across parks"
        );
    }

    /// H4/M4: an unmeasurable variant is charged so much that no budget admits
    /// it — and the limit of what this can check, stated.
    ///
    /// WHAT CANNOT BE TESTED: that each `#[non_exhaustive]` wildcard actually
    /// routes to `unmeasurable`. Constructing a variant this build does not
    /// know is impossible by definition, so no fixture can drive those arms.
    /// Changing one back to `_ => 0` is therefore invisible to every
    /// behavioural test — verified, not assumed. The count assertion below is a
    /// source scrape standing in for it, and is named as one.
    ///
    /// WHAT IS TESTED, because it is the half that decides the outcome: the
    /// charge is `usize::MAX`, so it exceeds EVERY budget. `MAX_PARKED_BYTES`
    /// would not: admission tests `parked_bytes + bytes > budget`, so on a host
    /// whose budget IS that value the first such item satisfies `MAX > MAX` as
    /// false and is admitted — the one item the charge exists to refuse.
    #[test]
    fn an_unmeasurable_variant_cannot_be_admitted_at_any_budget() {
        assert_eq!(
            unmeasurable("test", 0u8),
            usize::MAX,
            "an unmeasurable payload must be charged more than any budget, not \
             merely a large number"
        );

        let (mut ctx, _rx) = ctx();
        // The largest budget any host can have.
        assert!(
            matches!(
                ctx.park(
                    key(1),
                    continuation(),
                    ByteCount::new(unmeasurable("test", 0u8))
                ),
                ParkAdmission::Refused(_)
            ),
            "a park charged the unmeasurable rate must be REFUSED even on a \
             host at the clamp ceiling"
        );

        // SOURCE SCRAPE, with its limit stated above: every `#[non_exhaustive]`
        // enum this file measures must route its wildcard here. If you add or
        // remove one, change this count deliberately.
        let src = super::super::tests::strip_comments(include_str!("delegate_park.rs"));
        let cutoff = src.find("\nmod tests {").unwrap_or(src.len());
        let prod = &src[..cutoff];
        // Minus the definition itself, which also contains the needle — the
        // self-match trap this repo records twice.
        let call_sites =
            prod.matches("unmeasurable(").count() - prod.matches("fn unmeasurable(").count();
        assert_eq!(
            call_sites, 5,
            "the five `#[non_exhaustive]` enums measured here — DelegateRequest, \
             DelegateContainer, ContractContainer, InboundDelegateMsg, \
             UpdateData — must each charge an unknown variant as unmeasurable. \
             A wildcard that returns 0 instead is a silent bypass of this budget \
             on someone else's stdlib release"
        );
    }

    /// Every term of `request_bytes`, individually.
    ///
    /// The `ApplicationMessages` arm is the one every ordinary delegate call
    /// takes and it was entirely unpinned: both its inbound payloads and its
    /// params could be zeroed with the suite green.
    ///
    /// FALSIFY by zeroing any single term in `request_bytes`.
    #[test]
    fn every_request_bytes_term_is_charged() {
        use freenet_stdlib::prelude::{
            Delegate, DelegateCode, DelegateContainer, DelegateWasmAPIVersion,
        };
        let key = key(1);

        let bare = DelegateRequest::ApplicationMessages {
            key: key.clone(),
            params: Parameters::from(Vec::new()),
            inbound: Vec::new(),
        };
        let base = request_bytes(&bare);

        let with_inbound = DelegateRequest::ApplicationMessages {
            key: key.clone(),
            params: Parameters::from(Vec::new()),
            inbound: vec![InboundDelegateMsg::ApplicationMessage(
                freenet_stdlib::prelude::ApplicationMessage::new(vec![0u8; T_PAYLOAD])
                    .with_context(t_ctx()),
            )],
        };
        assert!(
            request_bytes(&with_inbound) >= base + T_PAYLOAD + T_CTX,
            "ApplicationMessages must charge its inbound payloads and contexts"
        );

        let with_params = DelegateRequest::ApplicationMessages {
            key: key.clone(),
            params: Parameters::from(vec![0u8; T_PARAMS]),
            inbound: Vec::new(),
        };
        assert!(
            request_bytes(&with_params) >= base + T_PARAMS,
            "ApplicationMessages must charge its params: they are \
             delegate-supplied and retained behind the park"
        );

        let code = DelegateCode::from(vec![1u8; T_CODE]);
        let params = Parameters::from(vec![2u8; T_PARAMS]);
        let container =
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate::from((&code, &params))));
        assert!(
            request_bytes(&DelegateRequest::RegisterDelegate {
                delegate: container,
                cipher: [0u8; 32],
                nonce: [0u8; 24],
            }) >= T_CODE + T_PARAMS,
            "RegisterDelegate must charge the whole container"
        );
        assert_eq!(
            request_bytes(&DelegateRequest::UnregisterDelegate(key)),
            0,
            "UnregisterDelegate carries a key and nothing else"
        );

        // THERE IS NO SECOND REGISTRATION VARIANT TO CHARGE. This test used to
        // assert that `RegisterDelegateWithPredecessors` charged its container
        // AND its predecessor list; that variant was removed from the wire in
        // freenet-stdlib 0.9.0 (freenet/freenet-stdlib#91, GHSA-824h-7x5x-wfmf)
        // and its node-side handler had already been disabled in #5199. Do not
        // restore this assertion by reintroducing the variant. If a future
        // registration variant does arrive, it lands on the `unmeasurable`
        // wildcard (charged as maximal, not free) until an arm is written for
        // it, which is what `an_unmeasurable_variant_cannot_be_admitted_at_any_budget`
        // holds.
    }

    /// P1b: a queued delegate re-registration is charged its PARAMETERS as well
    /// as its WASM.
    ///
    /// `DelegateContainer` exposes `code()` but no `params()`, so the helper
    /// counted the module and stopped. Parameters are delegate-supplied and
    /// bounded only by the ~100 MiB websocket message allowance, so eight large
    /// queued re-registrations bypassed the 64 MiB budget while each was
    /// charged as a tiny module.
    ///
    /// FALSIFY by dropping the `params()` term from `delegate_container_bytes`:
    /// the parameters here are 64x the code, so the assertion goes red on size
    /// rather than on a technicality.
    #[test]
    fn a_queued_registration_charges_its_parameters_not_just_its_wasm() {
        use freenet_stdlib::prelude::{
            Delegate, DelegateCode, DelegateContainer, DelegateWasmAPIVersion,
        };
        let code = DelegateCode::from(vec![1u8; 1024]);
        let params = Parameters::from(vec![2u8; 64 * 1024]);
        let delegate =
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate::from((&code, &params))));
        let req = DelegateRequest::RegisterDelegate {
            delegate,
            cipher: [0u8; 32],
            nonce: [0u8; 24],
        };
        assert!(
            request_bytes(&req) >= 1024 + 64 * 1024,
            "a registration must be charged its code AND its parameters; \
             charged {}",
            request_bytes(&req)
        );
    }

    /// P2 + P1a: `task_bytes` charges the upsert's CONTEXT, and RESERVES the
    /// off-loop fetch before it happens.
    ///
    /// The context is echoed back to the delegate, so it is retained exactly as
    /// long as the state is and runs to ~400 KiB. The fetch reserve is the
    /// larger of the two: fetched related states went into the sink and an
    /// unbounded channel with no accounting of any kind, so one park could
    /// retain ~2 GiB (4 upserts x 10 related x 50 MiB) against a nominal 64 MiB
    /// cap.
    ///
    /// FALSIFY by dropping either the `context` term or the `fetch_reserve`
    /// term from `task_bytes`.
    #[test]
    fn task_bytes_charges_the_upsert_context_and_reserves_its_fetch() {
        fn upsert(context: DelegateContext, missing: Vec<ContractInstanceId>) -> PendingUpsert {
            PendingUpsert {
                id: UpsertId::next(),
                key: ContractKey::from_params_and_code(
                    Parameters::from(vec![]),
                    freenet_stdlib::prelude::ContractCode::from(vec![0u8; 4]),
                ),
                update: Either::Right(StateDelta::from(vec![0u8; T_DELTA])),
                related_contracts: RelatedContracts::default(),
                code: None,
                is_put: false,
                context,
                missing,
            }
        }

        let baseline = task_bytes(
            &[],
            &[upsert(DelegateContext::default(), Vec::new())],
            TEST_ALLOWANCE,
        );

        let with_context = task_bytes(&[], &[upsert(t_ctx(), Vec::new())], TEST_ALLOWANCE);
        assert!(
            with_context >= baseline + T_CTX,
            "the upsert's echoed context must be charged: it is retained for \
             the life of the park exactly as the state is, and runs to ~400 \
             KiB. Charged {with_context} against a {baseline} baseline"
        );

        let with_fetch = task_bytes(
            &[],
            &[upsert(
                DelegateContext::default(),
                vec![ContractInstanceId::new([1u8; 32])],
            )],
            TEST_ALLOWANCE,
        );
        assert!(
            with_fetch >= baseline + TEST_ALLOWANCE,
            "an upsert naming missing related contracts must RESERVE its fetch \
             allowance BEFORE the fetch happens. Charging afterwards leaves \
             dropping what you already paid to retrieve as the only available \
             response. Charged {with_fetch} against a {baseline} baseline"
        );
    }

    /// The allowance SCALES with the budget and still leaves the cap binding,
    /// at both ends of the range.
    ///
    /// Asserts the BOUND, not the arithmetic, so retuning any of the three
    /// constants keeps it true. And asserts it at BOTH the floor and the
    /// ceiling, which the previous version did not: that one checked a single
    /// `const` against `MIN_PARKED_BYTES`, and the defect it missed was
    /// precisely that the allowance did not move with the budget at all — a
    /// 64 MiB node got the 8 MiB floor's share. A test pinned to one point
    /// cannot see a missing gradient.
    #[test]
    fn the_fetch_allowance_scales_with_the_budget_and_leaves_it_binding() {
        for budget in [MIN_PARKED_BYTES, MAX_PARKED_BYTES] {
            let per_upsert = upsert_fetch_allowance(budget);
            let worst_case_park = per_upsert * MAX_DEFERRED_UPSERTS_PER_PARK;
            assert!(
                per_upsert > 0,
                "a budget of {budget} must still allow SOME off-loop fetch, or \
                 every deferred upsert degrades to the inline path"
            );
            assert!(
                worst_case_park <= budget / 4,
                "one park's fetch reserve ({worst_case_park}) must leave room \
                 for others within its own budget ({budget}); a reserve sized \
                 to fill the budget turns every deferred upsert into an inline \
                 fallback"
            );
        }
        assert!(
            upsert_fetch_allowance(MAX_PARKED_BYTES) > upsert_fetch_allowance(MIN_PARKED_BYTES),
            "the allowance must SCALE with the node's budget. It did not: it \
             was a `const` derived from the floor, so a host with eight times \
             the budget got the same 256 KiB — the flat-cap defect \
             `parked_budget_for` exists to fix, one level down"
        );
    }

    fn key(byte: u8) -> DelegateKey {
        DelegateKey::new(
            [byte; 32],
            freenet_stdlib::prelude::CodeHash::new([byte; 32]),
        )
    }

    fn continuation() -> Continuation {
        Continuation {
            params: Parameters::from(Vec::new()),
            origin_contract: None,
            connection_scope: ConnectionScope::Local,
            user_context: None,
            inter_delegate: super::super::InterDelegateDispatch::Allowed,
            accumulated: Vec::new(),
            inbound_so_far: Vec::new(),
            responder: None,
            delivery: Delivery::Client,
            iterations: 0,
        }
    }

    fn pending(byte: u8) -> PendingRun {
        PendingRun::Client {
            id: EventId { id: byte as u64 },
            req: DelegateRequest::ApplicationMessages {
                key: key(byte),
                params: Parameters::from(Vec::new()),
                inbound: Vec::new(),
            },
            origin_contract: None,
            connection_scope: ConnectionScope::Local,
            user_context: None,
        }
    }

    fn ctx() -> (
        DelegateParkCtx,
        tokio::sync::mpsc::UnboundedReceiver<DelegateResume>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (DelegateParkCtx::new(tx), rx)
    }

    #[tokio::test]
    async fn park_then_take_round_trips() {
        let (mut ctx, _rx) = ctx();
        let k = key(1);
        assert!(!ctx.is_parked(&k));
        assert!(matches!(
            ctx.park(k.clone(), continuation(), ByteCount::default()),
            ParkAdmission::Admitted { .. }
        ));
        assert!(ctx.is_parked(&k));
        let (_cont, pend) = ctx
            .take_matching(&k, ctx.epoch_of(&k).expect("parked"))
            .expect("park must be takeable");
        assert!(pend.is_empty());
        assert!(!ctx.is_parked(&k), "take must end the park");
    }

    #[tokio::test]
    async fn node_wide_cap_refuses_and_hands_the_continuation_back() {
        let (mut ctx, _rx) = ctx();
        for i in 0..MAX_PARKED_DELEGATES {
            assert!(matches!(
                ctx.park(key(i as u8), continuation(), ByteCount::default()),
                ParkAdmission::Admitted { .. }
            ));
        }
        assert_eq!(ctx.parked_count(), MAX_PARKED_DELEGATES);
        // Over the cap: refused, and the continuation comes back so the caller
        // can fall back inline rather than losing the round-trip.
        assert!(matches!(
            ctx.park(key(200), continuation(), ByteCount::default()),
            ParkAdmission::Refused(_)
        ));
        assert!(!ctx.is_parked(&key(200)));
    }

    #[tokio::test]
    async fn double_park_is_refused_not_clobbered() {
        let (mut ctx, _rx) = ctx();
        let k = key(1);
        assert!(matches!(
            ctx.park(k.clone(), continuation(), ByteCount::default()),
            ParkAdmission::Admitted { .. }
        ));
        // The live continuation must survive: clobbering it would strand the
        // first round-trip's client responder.
        assert!(matches!(
            ctx.park(k.clone(), continuation(), ByteCount::default()),
            ParkAdmission::Refused(_)
        ));
        assert!(
            ctx.take_matching(&k, ctx.epoch_of(&k).expect("parked"))
                .is_some()
        );
    }

    #[tokio::test]
    async fn pending_queue_is_capped_and_overflow_is_returned_not_dropped() {
        let (mut ctx, _rx) = ctx();
        let k = key(1);
        ctx.park(k.clone(), continuation(), ByteCount::default());
        for i in 0..MAX_PENDING_PER_DELEGATE {
            assert!(matches!(
                ctx.queue_pending(&k, pending(i as u8)),
                QueueOutcome::Queued
            ));
        }
        // Overflow must hand the request BACK so the caller can answer it.
        // Silently dropping it would hang that client forever.
        assert!(matches!(
            ctx.queue_pending(&k, pending(99)),
            QueueOutcome::Rejected(_)
        ));
        let (_cont, pend) = ctx
            .take_matching(&k, ctx.epoch_of(&k).expect("parked"))
            .expect("park present");
        assert_eq!(pend.len(), MAX_PENDING_PER_DELEGATE);
    }

    #[tokio::test]
    async fn queueing_for_an_unparked_delegate_returns_the_request() {
        let (mut ctx, _rx) = ctx();
        assert!(matches!(
            ctx.queue_pending(&key(1), pending(1)),
            QueueOutcome::Rejected(_)
        ));
    }

    fn notification(contract: u8, state: &[u8]) -> PendingRun {
        let contract_id = ContractInstanceId::new([contract; 32]);
        PendingRun::Notification {
            contract_id,
            req: DelegateRequest::ApplicationMessages {
                key: key(1),
                params: Parameters::from(Vec::new()),
                inbound: vec![InboundDelegateMsg::ContractNotification(
                    freenet_stdlib::prelude::ContractNotification {
                        contract_id,
                        new_state: WrappedState::new(state.to_vec()),
                        context: DelegateContext::default(),
                    },
                )],
            },
        }
    }

    fn queued_state(run: &PendingRun) -> Option<Vec<u8>> {
        let PendingRun::Notification { req, .. } = run else {
            return None;
        };
        let DelegateRequest::ApplicationMessages { inbound, .. } = req else {
            return None;
        };
        #[allow(clippy::wildcard_enum_match_arm)]
        inbound.iter().find_map(|m| match m {
            InboundDelegateMsg::ContractNotification(n) => Some(n.new_state.as_ref().to_vec()),
            _ => None,
        })
    }

    /// Notifications COALESCE per contract instead of being rejected at the
    /// client queue's cap.
    ///
    /// Rejecting them would be a silent loss — a notification has no caller to
    /// return an error to — and it would land on exactly the wrong population:
    /// ghostkeys parks on prompts, so the window is precisely when a user is
    /// interacting, and Harvest with many address contracts subscribed would
    /// lose payment notifications there.
    #[tokio::test]
    async fn notifications_coalesce_per_contract_rather_than_being_rejected() {
        let (mut ctx, _rx) = ctx();
        let k = key(1);
        ctx.park(k.clone(), continuation(), ByteCount::default());

        // Far more than MAX_PENDING_PER_DELEGATE, all for ONE contract.
        for i in 0..(MAX_PENDING_PER_DELEGATE as u8 + 12) {
            assert!(
                matches!(
                    ctx.queue_pending(&k, notification(7, &[i])),
                    QueueOutcome::Queued
                ),
                "a notification must never be rejected for queue depth; \
                 superseded ones coalesce"
            );
        }

        let (_cont, pending) = ctx
            .take_matching(&k, ctx.epoch_of(&k).expect("parked"))
            .expect("park present");
        assert_eq!(
            pending.len(),
            1,
            "notifications for one contract must collapse to a single pending run"
        );
        assert_eq!(
            queued_state(&pending[0]),
            Some(vec![MAX_PENDING_PER_DELEGATE as u8 + 11]),
            "the NEWEST notification must win. Lossless only while the contract's \
             state is ACCUMULATING, so the newest subsumes the superseded — see \
             the precondition on `PendingRun::Notification`"
        );
    }

    /// A full client queue must not block notifications: the two lanes are
    /// separate, because only one of them has a caller that can be told.
    #[tokio::test]
    async fn a_full_client_queue_does_not_reject_notifications() {
        let (mut ctx, _rx) = ctx();
        let k = key(1);
        ctx.park(k.clone(), continuation(), ByteCount::default());

        for i in 0..MAX_PENDING_PER_DELEGATE {
            assert!(matches!(
                ctx.queue_pending(&k, pending(i as u8)),
                QueueOutcome::Queued
            ));
        }
        assert!(
            matches!(
                ctx.queue_pending(&k, pending(99)),
                QueueOutcome::Rejected(_)
            ),
            "client requests still hit the cap — the caller can be told"
        );
        assert!(
            matches!(
                ctx.queue_pending(&k, notification(3, b"x")),
                QueueOutcome::Queued
            ),
            "a notification must still be accepted with the client queue full"
        );

        let (_cont, pending_runs) = ctx
            .take_matching(&k, ctx.epoch_of(&k).expect("parked"))
            .expect("park present");
        assert_eq!(
            pending_runs.len(),
            MAX_PENDING_PER_DELEGATE + 1,
            "clients plus the coalesced notification"
        );
    }

    /// Distinct contracts are capped, so the coalescing map cannot grow without
    /// bound if subscriptions are not limited elsewhere.
    /// L9: refusals are COUNTED, not only logged. A refusal that increments
    /// nothing renders as a clean zero to anything reading metrics — the same
    /// pattern this branch fixed for the over-cap client request.
    #[tokio::test]
    async fn refusals_are_counted_per_cause() {
        let (mut ctx, _rx) = ctx();
        let k = key(1);
        ctx.park(k.clone(), continuation(), ByteCount::default());

        for i in 0..MAX_PENDING_PER_DELEGATE {
            ctx.queue_pending(&k, pending(i as u8));
        }
        ctx.queue_pending(&k, pending(99));
        for i in 0..MAX_PENDING_NOTIFICATION_CONTRACTS {
            ctx.queue_pending(&k, notification(i as u8, b"s"));
        }
        ctx.queue_pending(&k, notification(250, b"s"));

        let counts = ctx.refusals();
        assert_eq!(counts.client_requests, 1, "the over-cap client request");
        assert_eq!(
            counts.notifications, 1,
            "the over-cap notification contract"
        );
        assert_eq!(counts.parks, 0, "no park was refused here");
    }

    #[tokio::test]
    async fn distinct_notification_contracts_are_capped() {
        let (mut ctx, _rx) = ctx();
        let k = key(1);
        ctx.park(k.clone(), continuation(), ByteCount::default());

        for i in 0..MAX_PENDING_NOTIFICATION_CONTRACTS {
            assert!(matches!(
                ctx.queue_pending(&k, notification(i as u8, b"s")),
                QueueOutcome::Queued
            ));
        }
        assert!(
            matches!(
                ctx.queue_pending(&k, notification(250, b"s")),
                QueueOutcome::Rejected(_)
            ),
            "a NEW contract past the cap is refused; the delegate will see that \
             contract's next state change"
        );
        // An already-queued contract still coalesces at the cap.
        assert!(matches!(
            ctx.queue_pending(&k, notification(0, b"newer")),
            QueueOutcome::Queued
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn park_expires_only_after_the_ttl() {
        let (mut ctx, mut rx) = ctx();
        let mut buffered = VecDeque::new();
        let k = key(1);
        ctx.park(k.clone(), continuation(), ByteCount::default());

        tokio::time::advance(PARK_TTL - Duration::from_secs(1)).await;
        assert!(
            ctx.expired(tokio::time::Instant::now(), &mut rx, &mut buffered)
                .is_empty(),
            "must not expire early — a park cut short would report a spurious \
             failure for work that was about to succeed"
        );

        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(
            ctx.expired(tokio::time::Instant::now(), &mut rx, &mut buffered),
            vec![(k.clone(), ctx.epoch_of(&k).expect("parked"))]
        );
    }

    /// #5554: the backstop must NOT sweep a park whose resume is already in the
    /// loop's hands — because sweeping it throws away a human's Allow.
    ///
    /// This is the one case the rest of the suite could not see. The guard tests
    /// prove `deliver()` preserves the answer the user gave;
    /// `a_stale_resume_from_a_force_resumed_park_is_rejected` proves a resume
    /// arriving after a sweep is DISCARDED (correct, from the registry's point
    /// of view). Neither asks what the discarded resume was CARRYING. Put both
    /// facts in one room and the answer is gone: the sweep ends the park without
    /// consuming the guard, the guard's resume is then rejected on epoch, and
    /// the delegate is re-entered with `inbound: Vec::new()` — told nothing
    /// about the prompt it asked, which for a delegate that branches on the
    /// answer is worse than a denial.
    ///
    /// It is reachable in ordinary operation: `PARK_WORK_BUDGET < PARK_TTL`
    /// guarantees the off-loop TASK finishes in time, NOT that the loop DRAINS
    /// its resume in time. The drain is capped at `MAX_RESUME_DRAIN_BATCH` (16)
    /// while one `handle_delegate_resume` can cost 25 runs, and the sweep runs
    /// in the same iteration, immediately after.
    ///
    /// FALSIFY by dropping the `already_delivered` filter from `expired`: the
    /// first assertion then reports the park as expired. The third assertion is
    /// the counterfactual that keeps the first from passing vacuously — with an
    /// empty buffer this very park IS swept, so the exclusion is doing the work.
    #[tokio::test(start_paused = true)]
    async fn the_backstop_leaves_a_park_whose_answer_is_already_in_hand() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut ctx = DelegateParkCtx::new(tx.clone());
        let k = key(1);
        let ParkAdmission::Admitted { epoch } =
            ctx.park(k.clone(), continuation(), ByteCount::default())
        else {
            panic!("park must be admitted");
        };

        // The human clicks Allow and the off-loop task's guard delivers.
        let (answers, fetches) = sinks();
        answers.lock().unwrap().push(answer(1));
        drop(ParkGuard::new(
            tx,
            k.clone(),
            epoch,
            vec![1],
            Vec::new(),
            answers,
            fetches,
        ));

        // The loop takes it off the channel but runs out of budget before
        // running it, so it sits in the buffer — and the park is still parked.
        let mut buffered: VecDeque<DelegateResume> = VecDeque::new();
        while let Ok(resume) = rx.try_recv() {
            buffered.push_back(resume);
        }
        assert_eq!(buffered.len(), 1, "the guard must have delivered a resume");

        tokio::time::advance(PARK_TTL + Duration::from_secs(1)).await;
        assert!(
            ctx.expired(tokio::time::Instant::now(), &mut rx, &mut buffered)
                .is_empty(),
            "a park whose resume is already buffered is QUEUED, not wedged; \
             force-resuming it discards the answer that resume is carrying \
             (#5554)"
        );

        // ...and what it is carrying really is the human's answer, not a denial.
        let InboundDelegateMsg::UserResponse(response) = buffered[0]
            .inbound
            .iter()
            .find(|m| matches!(m, InboundDelegateMsg::UserResponse(r) if r.request_id == 1))
            .expect("the buffered resume must carry the answer for request 1")
        else {
            unreachable!()
        };
        assert_eq!(
            &response.response[..],
            b"allow".as_slice(),
            "this is the answer the sweep would have thrown away"
        );

        // The counterfactual: the park IS past its TTL. Without the buffer to
        // consult, the backstop sweeps it — so the exclusion above is load-
        // bearing rather than a park that was never expiring.
        let mut nothing_in_hand = VecDeque::new();
        let (_unused_tx, mut empty_rx) = tokio::sync::mpsc::unbounded_channel();
        assert_eq!(
            ctx.expired(
                tokio::time::Instant::now(),
                &mut empty_rx,
                &mut nothing_in_hand
            ),
            vec![(k.clone(), epoch)],
            "the park really is past PARK_TTL"
        );
    }

    /// #5554 round 2: a resume that arrives AFTER the loop's batch snapshot,
    /// while the loop is awaiting, must still stop the sweep.
    ///
    /// The first fix took a `&VecDeque` snapshot and left the caller to fill it,
    /// which reads as atomic and is not: the loop drains, then AWAITS a batch of
    /// resumes, then sweeps. A `ParkGuard` firing during that await puts its
    /// resume in the CHANNEL, and a buffer snapshotted beforehand cannot see it
    /// — so the sweep force-resumed a park whose answer had already arrived.
    /// Bit-for-bit the original bug, on a window that reaches
    /// `USER_INPUT_TIMEOUT` (60 s) when a full park table sends a resume down
    /// the inline prompt path, which is exactly the condition that makes
    /// resumes queue in the first place.
    ///
    /// This test models that sequence: the buffer is snapshotted EMPTY, the
    /// guard fires afterwards, and only then is the sweep asked. It fails if
    /// `expired` trusts what it was handed instead of re-reading the channel.
    ///
    /// It is the property test the source pin could not be. A pin asserting the
    /// drain precedes the sweep is satisfied by the buggy code — the drain DID
    /// precede it, with an await in between. **Position cannot express
    /// duration**, so the ordering has to be enforced by the signature (which
    /// takes the receiver) and checked behaviourally (here).
    ///
    /// FALSIFY by making `expired` skip its `absorb_delivered` call and trust
    /// `already_delivered` as passed.
    #[tokio::test(start_paused = true)]
    async fn a_resume_arriving_after_the_snapshot_still_stops_the_sweep() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut ctx = DelegateParkCtx::new(tx.clone());
        let k = key(1);
        let ParkAdmission::Admitted { epoch } =
            ctx.park(k.clone(), continuation(), ByteCount::default())
        else {
            panic!("park must be admitted");
        };

        // The loop's snapshot: nothing has been delivered yet.
        let mut buffered: VecDeque<DelegateResume> = VecDeque::new();
        while let Ok(resume) = rx.try_recv() {
            buffered.push_back(resume);
        }
        assert!(
            buffered.is_empty(),
            "the snapshot must be taken BEFORE the guard fires, or this test \
             is the buffered case again rather than the racing one"
        );

        // ...and NOW the human answers, while the loop is inside its batch.
        let (answers, fetches) = sinks();
        answers.lock().unwrap().push(answer(1));
        drop(ParkGuard::new(
            tx,
            k.clone(),
            epoch,
            vec![1],
            Vec::new(),
            answers,
            fetches,
        ));

        tokio::time::advance(PARK_TTL + Duration::from_secs(1)).await;
        assert!(
            ctx.expired(tokio::time::Instant::now(), &mut rx, &mut buffered)
                .is_empty(),
            "the answer arrived after the snapshot but BEFORE the sweep; \
             force-resuming now discards the human's response, which is the \
             whole defect (#5554)"
        );
        assert_eq!(
            answered_ids(&buffered[0]),
            vec![1],
            "and the resume it declined to sweep is the one carrying the answer"
        );
    }

    /// The same defect one scope in: the sweep LOOP awaits too.
    ///
    /// `expired` returns a list, and force-resuming the first entry re-enters
    /// WASM. A guard firing during that await is invisible to the decision
    /// already made about the second entry, so the list is stale by the time it
    /// is used. `should_force_resume` re-asks per victim, immediately before
    /// each force-resume, with no `.await` in between.
    ///
    /// FALSIFY by making `should_force_resume` skip its `absorb_delivered`
    /// call, or by having the loop trust `expired`'s list.
    #[tokio::test(start_paused = true)]
    async fn a_resume_arriving_during_an_earlier_force_resume_cancels_the_next() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut ctx = DelegateParkCtx::new(tx.clone());
        let (first, second) = (key(1), key(2));
        let ParkAdmission::Admitted { epoch: e1 } =
            ctx.park(first.clone(), continuation(), ByteCount::default())
        else {
            panic!("park must be admitted");
        };
        let ParkAdmission::Admitted { epoch: e2 } =
            ctx.park(second.clone(), continuation(), ByteCount::default())
        else {
            panic!("park must be admitted");
        };

        let mut buffered: VecDeque<DelegateResume> = VecDeque::new();
        tokio::time::advance(PARK_TTL + Duration::from_secs(1)).await;
        let victims = ctx.expired(tokio::time::Instant::now(), &mut rx, &mut buffered);
        assert_eq!(
            victims,
            vec![(first.clone(), e1), (second.clone(), e2)],
            "both parks are past the TTL with nothing in hand"
        );

        // The loop force-resumes the FIRST victim. That awaits, and during it
        // the second park's human answers.
        let (answers, fetches) = sinks();
        answers.lock().unwrap().push(answer(9));
        drop(ParkGuard::new(
            tx,
            second.clone(),
            e2,
            vec![9],
            Vec::new(),
            answers,
            fetches,
        ));

        assert!(
            !ctx.should_force_resume(&second, e2, &mut rx, &mut buffered),
            "the second victim's answer landed while the first was being \
             force-resumed; sweeping it now throws that answer away (#5554)"
        );
        assert!(
            ctx.should_force_resume(&first, e1, &mut rx, &mut buffered),
            "the first victim produced nothing, so it is still genuinely \
             wedged and the backstop must still fire for it — otherwise this \
             check would disarm the backstop rather than target it"
        );
    }

    /// The exclusion matches on `(key, epoch)`, not key alone.
    ///
    /// A buffered resume from an EARLIER park of the same delegate is stale: its
    /// park was already ended, it carries nothing the CURRENT park is owed, and
    /// letting it shield the current one would disarm the backstop for exactly
    /// the delegate that has already needed it once — a genuinely wedged park
    /// would then stay wedged forever.
    ///
    /// FALSIFY by dropping the `resume.epoch == entry.epoch` half of the filter:
    /// the sweep then returns empty.
    #[tokio::test(start_paused = true)]
    async fn a_stale_buffered_resume_does_not_shield_the_current_park() {
        let (tx, mut _rx) = tokio::sync::mpsc::unbounded_channel();
        let mut ctx = DelegateParkCtx::new(tx);
        let k = key(1);

        let ParkAdmission::Admitted { epoch: first } =
            ctx.park(k.clone(), continuation(), ByteCount::default())
        else {
            panic!("first park must be admitted");
        };
        // The backstop ended park #1; the delegate re-parked.
        assert!(ctx.take_matching(&k, first).is_some());
        let ParkAdmission::Admitted { epoch: second } =
            ctx.park(k.clone(), continuation(), ByteCount::default())
        else {
            panic!("second park must be admitted");
        };

        // Park #1's guard finally fires, and its resume lands in the buffer.
        let mut buffered: VecDeque<DelegateResume> = VecDeque::new();
        buffered.push_back(DelegateResume {
            delegate_key: k.clone(),
            epoch: first,
            cause: ResumeCause::Completed,
            inbound: vec![answer(1)],
            upserts: Vec::new(),
            unresolved_upserts: Vec::new(),
        });

        tokio::time::advance(PARK_TTL + Duration::from_secs(1)).await;
        assert_eq!(
            ctx.expired(tokio::time::Instant::now(), &mut _rx, &mut buffered),
            vec![(k.clone(), second)],
            "a resume for the PREVIOUS park says nothing about this one; the \
             backstop must still fire"
        );
    }

    /// H1: a resume from a park the TTL backstop already ended must be REJECTED,
    /// not absorbed by whatever park exists now.
    ///
    /// The sweep force-resumes a park without consuming the off-loop task's
    /// `ParkGuard`, so that guard still owes a resume. If the delegate has
    /// re-parked by the time it lands, matching on key alone hands the OLD
    /// continuation's `UserResponse`/`PutContractResponse` messages to the NEW
    /// park — the cross-round-trip corruption the whole exclusion exists to
    /// prevent, arriving through the backstop I was asked to add.
    ///
    /// FALSIFY by making `take_matching` ignore the epoch: the stale resume is
    /// then absorbed and this returns `Some`.
    #[tokio::test]
    async fn a_stale_resume_from_a_force_resumed_park_is_rejected() {
        let (mut ctx, _rx) = ctx();
        let k = key(1);

        let ParkAdmission::Admitted { epoch: first } =
            ctx.park(k.clone(), continuation(), ByteCount::default())
        else {
            panic!("first park must be admitted");
        };

        // The TTL backstop ends park #1 WITHOUT consuming its guard.
        assert!(
            ctx.take_matching(&k, first).is_some(),
            "the sweep ends the park it observed"
        );

        // The delegate re-parks: a new round-trip, a new continuation.
        let ParkAdmission::Admitted { epoch: second } =
            ctx.park(k.clone(), continuation(), ByteCount::default())
        else {
            panic!("second park must be admitted");
        };
        assert_ne!(first, second, "each park must have its own identity");

        // Park #1's guard finally fires. It must NOT take park #2.
        assert!(
            ctx.take_matching(&k, first).is_none(),
            "a stale resume must be rejected; absorbing it would feed park #1's \
             messages into park #2 (#5544 H1)"
        );
        assert_eq!(
            ctx.epoch_of(&k),
            Some(second),
            "the live park must survive the stale resume untouched"
        );
    }

    /// The context attached to a message is CHARGED. `DelegateContext` runs to
    /// nearly 400 KiB, so a client can queue small-payload messages carrying
    /// large contexts behind a park and move `parked_bytes` almost not at all.
    ///
    /// FALSIFY by dropping the `msg.get_context()` term from `inbound_bytes`.
    #[tokio::test]
    async fn message_contexts_are_charged_not_just_payloads() {
        let big_ctx = DelegateContext::new(vec![0u8; 200 * 1024]);
        let tiny_payload = InboundDelegateMsg::ApplicationMessage(
            freenet_stdlib::prelude::ApplicationMessage::new(vec![1u8; 8])
                .with_context(big_ctx.clone()),
        );
        let mut cont = continuation();
        cont.inbound_so_far = vec![tiny_payload];
        assert!(
            continuation_bytes(&cont) >= 200 * 1024,
            "a message's context must be charged; payload was 8 bytes and the \
             context 200 KiB, and only the context makes this a real cost"
        );
    }

    /// Coalesced notifications drain in ARRIVAL order, not hash order.
    ///
    /// Each drained notification executes delegate WASM and can mutate secrets
    /// and contracts, so hash order lets observable effects reorder between
    /// runs and identical simulation runs diverge.
    ///
    /// FALSIFY by draining `pending_notifications` directly instead of through
    /// `notification_order`.
    #[tokio::test]
    async fn coalesced_notifications_drain_in_arrival_order() {
        let (mut ctx, _rx) = ctx();
        let k = key(1);
        ctx.park(k.clone(), continuation(), ByteCount::default());

        // Insert in a fixed order; supersede one in the middle to confirm
        // coalescing keeps its ORIGINAL slot rather than moving it to the back.
        let arrival: Vec<u8> = (0..8).collect();
        for c in &arrival {
            ctx.queue_pending(&k, notification(*c, b"first"));
        }
        ctx.queue_pending(&k, notification(3, b"second"));

        let epoch = ctx.epoch_of(&k).expect("parked");
        let (_cont, pending) = ctx.take_matching(&k, epoch).expect("parked");
        let drained: Vec<u8> = pending
            .iter()
            .filter_map(|run| match run {
                PendingRun::Notification { contract_id, .. } => Some(contract_id.as_bytes()[0]),
                PendingRun::Client { .. } => None,
            })
            .collect();
        assert_eq!(
            drained, arrival,
            "notifications must drain in arrival order, and a superseded one \
             must keep its original position"
        );
    }

    /// H1/H2: EVERY variant that can carry a context is charged for it.
    ///
    /// One test per variant, deliberately. The previous single test used
    /// `ApplicationMessage` — the one variant the stdlib `get_context()`
    /// accessor DOES cover — so it asserted the property on the only case that
    /// already worked, while `UserResponse` (client-supplied, ~400 KiB) and
    /// `ContextUpdated` (whose payload IS a context) were charged zero through
    /// that accessor's `_ => None`.
    ///
    /// FALSIFY: drop any single `ctx_len(..)` term and its row here fails.
    #[tokio::test]
    async fn every_context_carrying_variant_is_charged() {
        const N: usize = 64 * 1024;
        let ctx = DelegateContext::new(vec![0u8; N]);
        let cid = ContractInstanceId::new([1; 32]);

        let inbound: Vec<(&str, InboundDelegateMsg<'static>)> = vec![
            (
                "ApplicationMessage",
                InboundDelegateMsg::ApplicationMessage(
                    freenet_stdlib::prelude::ApplicationMessage::new(Vec::new())
                        .with_context(ctx.clone()),
                ),
            ),
            (
                // The one the accessor does not even list.
                "UserResponse",
                InboundDelegateMsg::UserResponse(freenet_stdlib::prelude::UserInputResponse {
                    request_id: 1,
                    response: freenet_stdlib::prelude::ClientResponse::new(Vec::new()),
                    context: ctx.clone(),
                }),
            ),
            (
                "GetContractResponse",
                InboundDelegateMsg::GetContractResponse(
                    freenet_stdlib::prelude::GetContractResponse {
                        contract_id: cid,
                        state: None,
                        context: ctx.clone(),
                    },
                ),
            ),
            (
                "ContractNotification",
                InboundDelegateMsg::ContractNotification(
                    freenet_stdlib::prelude::ContractNotification {
                        contract_id: cid,
                        new_state: WrappedState::new(Vec::new()),
                        context: ctx.clone(),
                    },
                ),
            ),
        ];
        for (name, msg) in inbound {
            let mut cont = continuation();
            cont.inbound_so_far = vec![msg];
            assert!(
                continuation_bytes(&cont) >= N,
                "{name}: its context must be charged; payload was empty, so only \
                 the context makes this a real cost"
            );
        }

        // Outbound: `ContextUpdated` is the accessor's other blind spot, and it
        // accumulates across parks via `RunSeed.accumulated`.
        let mut cont = continuation();
        cont.accumulated = vec![OutboundDelegateMsg::ContextUpdated(ctx.clone())];
        assert!(
            continuation_bytes(&cont) >= N,
            "ContextUpdated's payload IS a context and must be charged"
        );

        let mut cont = continuation();
        cont.accumulated = vec![OutboundDelegateMsg::ApplicationMessage(
            freenet_stdlib::prelude::ApplicationMessage::new(Vec::new()).with_context(ctx),
        )];
        assert!(
            continuation_bytes(&cont) >= N,
            "an outbound ApplicationMessage's context must be charged"
        );
    }

    /// P1a: the byte cap must charge what is actually RETAINED, including the
    /// payloads the off-loop task holds, not just what `Continuation` points at.
    ///
    /// FALSIFY by reverting any of the three: dropping `task_bytes` from
    /// `park`, omitting `params` from `continuation_bytes`, or removing the
    /// projected-total check on the client queue lane.
    #[tokio::test]
    async fn the_byte_cap_charges_retained_payloads_not_just_the_continuation() {
        let (mut ctx, _rx) = ctx();

        // A continuation carrying a large inbound state, as a real parked GET
        // response would.
        let big = vec![0u8; 8 * 1024 * 1024];
        let mut cont = continuation();
        cont.inbound_so_far = vec![InboundDelegateMsg::ContractNotification(
            freenet_stdlib::prelude::ContractNotification {
                contract_id: ContractInstanceId::new([1; 32]),
                new_state: WrappedState::new(big.clone()),
                context: DelegateContext::default(),
            },
        )];
        assert!(
            continuation_bytes(&cont) >= big.len(),
            "the continuation's inbound state must be charged"
        );

        // `params` is delegate-supplied and retained; omitting it was one of the
        // three ways this bound failed to bound.
        let mut with_params = continuation();
        with_params.params = Parameters::from(vec![7u8; 4096]);
        assert!(
            continuation_bytes(&with_params) >= 4096,
            "`params` must be charged: it is retained for the life of the park"
        );

        // Fill the budget with parks that each carry a large task payload, and
        // confirm admission is refused rather than the total silently growing.
        let per_park = ctx.budget() / 4;
        let mut admitted = 0usize;
        for i in 0..MAX_PARKED_DELEGATES {
            match ctx.park(key(i as u8), continuation(), ByteCount::new(per_park)) {
                ParkAdmission::Admitted { .. } => admitted += 1,
                ParkAdmission::Refused(_) => break,
            }
        }
        assert!(
            admitted <= 4,
            "the byte cap must refuse once the RETAINED total is reached; \
             admitted {admitted} parks of {per_park} bytes each against a \
             {} byte budget",
            ctx.budget()
        );
    }

    type Sinks = (
        std::sync::Arc<std::sync::Mutex<Vec<InboundDelegateMsg<'static>>>>,
        std::sync::Arc<std::sync::Mutex<Vec<ResolvedUpsert>>>,
    );

    fn sinks() -> Sinks {
        (Default::default(), Default::default())
    }

    fn answer(request_id: u32) -> InboundDelegateMsg<'static> {
        InboundDelegateMsg::UserResponse(freenet_stdlib::prelude::UserInputResponse {
            request_id,
            response: freenet_stdlib::prelude::ClientResponse::new(b"allow".to_vec()),
            context: DelegateContext::default(),
        })
    }

    // `InboundDelegateMsg` is `#[non_exhaustive]`, so the wildcard is required
    // rather than lazy, and this helper genuinely wants only `UserResponse`.
    // The attribute has to sit on the EXPRESSION, not on the arm: on the arm it
    // does not suppress the lint, which is only visible in CI because the crate
    // warns on this locally and denies it under `-D warnings`.
    #[allow(clippy::wildcard_enum_match_arm)]
    fn answered_ids(resume: &DelegateResume) -> Vec<u32> {
        resume
            .inbound
            .iter()
            .filter_map(|m| match m {
                InboundDelegateMsg::UserResponse(r) => Some(r.request_id),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn guard_delivers_exactly_one_resume_on_success() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (answers, fetches) = sinks();
        let guard = ParkGuard::new(tx, key(1), 0, Vec::new(), Vec::new(), answers, fetches);
        guard.send();
        let resume = rx.recv().await.expect("one resume");
        assert_eq!(resume.cause, ResumeCause::Completed);
        assert!(rx.try_recv().is_err(), "must not deliver twice");
    }

    /// The `Drop` path must SYNTHESIZE the terminal results it owes.
    ///
    /// The previous version of this test built the guard with EMPTY owed lists
    /// and then asserted `resume.inbound.is_empty()`. That is correct for the
    /// case it constructed and the exact OPPOSITE of what the code must do when
    /// prompts are owed — so it pinned the ABSENCE of the behaviour, and a
    /// reader took the assertion as the contract. Mutation testing found the
    /// whole synthesis block could be deleted with the suite still green.
    #[tokio::test]
    async fn drop_synthesizes_denials_for_everything_it_owes() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (answers, fetches) = sinks();
        drop(ParkGuard::new(
            tx,
            key(1),
            0,
            vec![1, 2],
            vec![OwedUpsert {
                id: UpsertId::next(),
                contract: ContractInstanceId::new([3; 32]),
                is_put: true,
                context: DelegateContext::new(b"correlation-token".to_vec()),
            }],
            answers,
            fetches,
        ));
        let resume = rx.recv().await.expect("drop must still resume the park");
        assert_eq!(resume.cause, ResumeCause::TimedOut);
        assert_eq!(
            answered_ids(&resume),
            vec![1, 2],
            "every owed prompt must get a synthesized response, or the delegate \
             waits forever for one nothing remains to produce"
        );
        assert_eq!(resume.unresolved_upserts.len(), 1);
        let owed = &resume.unresolved_upserts[0];
        assert_eq!(owed.contract, ContractInstanceId::new([3; 32]));
        assert!(owed.is_put);
        assert_eq!(
            owed.context.as_ref(),
            b"correlation-token",
            "the synthesized failure must echo the context the DELEGATE sent. \
             It is how a delegate correlates a response with the request that \
             produced it, so a defaulted one can be applied to the wrong \
             logical request, and is unusable outright when two requests target \
             the same contract"
        );
    }

    /// The owed list must carry the context the DELEGATE sent.
    ///
    /// This is the assertion that was missing. The guard-level tests build a
    /// `ParkGuard` directly, so they pin that the guard preserves what it is
    /// GIVEN — and the bug was in what the caller gave it. Defaulting the
    /// context where this list used to be built inline left every one of them
    /// green.
    ///
    /// FALSIFY by defaulting `context` in `owed_upserts`.
    #[test]
    fn owed_upserts_carry_the_delegate_s_context() {
        let key = ContractKey::from_params_and_code(
            Parameters::from(vec![]),
            freenet_stdlib::prelude::ContractCode::from(vec![0u8; 4]),
        );
        let owed = owed_upserts(&[PendingUpsert {
            id: UpsertId::next(),
            key,
            update: Either::Right(StateDelta::from(vec![])),
            related_contracts: RelatedContracts::default(),
            code: None,
            is_put: true,
            context: DelegateContext::new(b"correlation-token".to_vec()),
            missing: Vec::new(),
        }]);
        assert_eq!(owed.len(), 1);
        assert_eq!(owed[0].contract, *key.id());
        assert!(owed[0].is_put);
        assert_eq!(
            owed[0].context.as_ref(),
            b"correlation-token",
            "the owed record must carry the delegate's own context: it is what \
             a synthesized failure echoes, and it is how the delegate tells \
             WHICH request failed when two target the same contract"
        );
    }

    /// The context survives RECONCILIATION, not just construction.
    ///
    /// The owed list is matched against completions as a MULTISET on
    /// `(contract, is_put)`, so two upserts naming one contract are
    /// indistinguishable by key — which is exactly the case where a defaulted
    /// context is unusable rather than merely unhelpful. Here one of the two
    /// completes and the other does not; the survivor must carry ITS OWN
    /// context through.
    ///
    /// FALSIFY by reconciling on `(contract, is_put)` again instead of on
    /// `id`: the later completion then cancels the earlier's obligation and the
    /// count goes to 0.
    ///
    /// **NOT by defaulting the context**, which is what this line used to say
    /// and which is false of THIS test. It constructs `OwedUpsert` by hand, so
    /// it never runs the production builder — the trap documented on
    /// `owed_upserts` twenty lines up, written into the instruction for
    /// checking the fix for it. Verified by doing exactly that: defaulting the
    /// context in `owed_upserts` leaves this test GREEN, while
    /// `owed_upserts_carry_the_delegate_s_context` goes red, which is the test
    /// that actually covers it.
    ///
    /// A WRONG `FALSIFY` LINE IS WORSE THAN A WEAK TEST. A weak test fails to
    /// catch a regression; an instruction like this one recruits the next
    /// person into confirming something false — they follow it, see the result
    /// it predicts, and come away more confident than if nothing had been
    /// written. It converts a gap into a positive belief.
    #[tokio::test]
    async fn a_partially_resolved_upsert_pair_keeps_the_unresolved_one_s_context() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (answers, fetches) = sinks();
        // Derive the id FROM the key the resolved upsert will carry, so the
        // reconciliation really does match it. Building the two independently
        // makes the completion match nothing and the test pass for the wrong
        // reason.
        let resolved_key = ContractKey::from_params_and_code(
            Parameters::from(vec![]),
            freenet_stdlib::prelude::ContractCode::from(vec![0u8; 4]),
        );
        let contract = *resolved_key.id();
        let resolved_id = UpsertId::next();
        let guard = ParkGuard::new(
            tx,
            key(1),
            0,
            Vec::new(),
            vec![
                OwedUpsert {
                    id: resolved_id,
                    contract,
                    is_put: true,
                    context: DelegateContext::new(b"first".to_vec()),
                },
                OwedUpsert {
                    id: UpsertId::next(),
                    contract,
                    is_put: true,
                    context: DelegateContext::new(b"second".to_vec()),
                },
            ],
            answers,
            fetches.clone(),
        );
        // Exactly ONE of the pair resolves.
        fetches.lock().unwrap().push(ResolvedUpsert {
            pending: PendingUpsert {
                id: resolved_id,
                key: resolved_key,
                update: Either::Right(StateDelta::from(vec![])),
                related_contracts: RelatedContracts::default(),
                code: None,
                is_put: true,
                context: DelegateContext::new(b"first".to_vec()),
                missing: Vec::new(),
            },
            fetched: FetchDisposition::Resolved(Ok(Vec::new())),
        });
        drop(guard);

        let resume = rx.recv().await.expect("drop must still resume the park");
        assert_eq!(
            resume.unresolved_upserts.len(),
            1,
            "one of the pair resolved, so exactly one obligation must remain"
        );
        assert!(
            !resume.unresolved_upserts[0].context.as_ref().is_empty(),
            "the surviving obligation must still carry a context; a defaulted \
             one cannot be told from the resolved sibling's"
        );
    }

    /// F2: answers a human ALREADY GAVE must survive the `Drop` path.
    ///
    /// The sinks used to be created inside the spawned future, so `Drop` saw
    /// none of them and rewrote every owed prompt as a denial. The user clicks
    /// Allow, the task panics, and the delegate is told denied.
    #[tokio::test]
    async fn drop_keeps_answers_already_given_rather_than_denying_them() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (answers, fetches) = sinks();
        answers.lock().unwrap().push(answer(1)); // the human said allow
        drop(ParkGuard::new(
            tx,
            key(1),
            0,
            vec![1, 2],
            Vec::new(),
            answers,
            fetches,
        ));
        let resume = rx.recv().await.expect("resume");
        let kept: Vec<&InboundDelegateMsg<'static>> = resume
            .inbound
            .iter()
            .filter(|m| matches!(m, InboundDelegateMsg::UserResponse(r) if r.request_id == 1))
            .collect();
        assert_eq!(kept.len(), 1, "exactly one response for request 1");
        let InboundDelegateMsg::UserResponse(r) = kept[0] else {
            unreachable!()
        };
        assert_eq!(
            &r.response[..],
            b"allow".as_slice(),
            "the answer the human gave must survive, not be replaced by a denial"
        );
        assert_eq!(
            answered_ids(&resume),
            vec![1, 2],
            "the unanswered one is still synthesized"
        );
    }

    /// F1/F3: the reconciliation is over a MULTISET, not a set.
    ///
    /// `request_id` is chosen by delegate WASM, and `deferred_upserts` is built
    /// by two independent loops with no de-duplication, so duplicates on both
    /// lists are reachable. Filtering by membership let ONE completion cancel
    /// the obligation for BOTH, and the second waited forever.
    #[tokio::test]
    async fn reconciliation_counts_duplicates_rather_than_matching_by_membership() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (answers, fetches) = sinks();
        answers.lock().unwrap().push(answer(7)); // only ONE of the two owed
        let contract = ContractInstanceId::new([9; 32]);
        drop(ParkGuard::new(
            tx,
            key(1),
            0,
            vec![7, 7],
            vec![
                OwedUpsert {
                    id: UpsertId::next(),
                    contract,
                    is_put: true,
                    context: DelegateContext::default(),
                },
                OwedUpsert {
                    id: UpsertId::next(),
                    contract,
                    is_put: true,
                    context: DelegateContext::default(),
                },
            ],
            answers,
            fetches,
        ));
        let resume = rx.recv().await.expect("resume");
        assert_eq!(
            answered_ids(&resume),
            vec![7, 7],
            "two owed prompts with the SAME id need two responses; matching by \
             membership would have cancelled both obligations with one answer"
        );
        assert_eq!(
            resume.unresolved_upserts.len(),
            2,
            "two owed upserts on one contract need two outcomes"
        );
    }
}
