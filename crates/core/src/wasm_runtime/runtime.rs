use super::{
    RuntimeResult,
    contract_store::ContractStore,
    delegate_api::DelegateApiVersion,
    delegate_store::DelegateStore,
    engine::{BackendEngine, Engine, InstanceHandle, WasmEngine},
    error::RuntimeInnerError,
    native_api,
    secrets_store::SecretsStore,
};
use freenet_stdlib::{
    memory::{
        WasmLinearMem,
        buf::{BufferBuilder, BufferMut},
    },
    prelude::*,
};
use std::path::Path;
use std::sync::atomic::AtomicI64;
use std::sync::{Arc, Mutex};

use super::ModuleCache;

/// A compiled WASM module cache shared across multiple `Runtime` instances.
///
/// The backend is wasmtime: a `Module` owns its compiled machine code via an
/// internal `Arc<CodeMemory>`, so clones are cheap (an Arc refcount bump) and
/// dropping the last clone frees the compiled code (verified by
/// `wasmtime_engine::tests::test_module_drop_frees_memory`). Sharing one cache
/// across the `RuntimePool` avoids compiling and storing the same contract N
/// times (once per pool executor).
///
/// The cache is bounded by the total compiled **byte size** of its entries
/// (see [`ModuleCache`] and
/// [`default_module_cache_budget_bytes`](super::default_module_cache_budget_bytes)),
/// not by a fixed entry count. A byte budget is the correct bound here because:
///
/// - It scales with how many contracts a node actually hosts: a node hosting
///   thousands of small contracts no longer thrashes the way the old 1024-entry
///   *count* cap did (the eviction-recompilation cycle behind issue #4441).
/// - It bounds the cache's absolute memory footprint regardless of contract
///   count, which a count cap could not (1024 large modules ≫ 1024 small ones).
///
/// The contract cache is keyed by [`CodeHash`], NOT by `ContractKey`. Compilation
/// only ever sees the WASM code (`engine.compile(code)`); parameters are written
/// into linear memory at call time. Keying by `ContractKey` — whose `Hash`/`Eq`
/// compare `instance = blake3(code_hash ‖ params)` — therefore compiled and
/// retained one copy of identical machine code per PARAMETER SET: a measured
/// ~17x duplication on a production gateway (3,746 cached modules for 215
/// distinct `.wasm` files), which is issue #5268's largest single contributor to
/// peers being OOM-killed at the shipped 2 GiB `MemoryMax`. The source-bytes
/// cache one layer down (`ContractStore::contract_cache`) already keys this way.
pub(crate) type SharedModuleCache<K> = Arc<Mutex<ModuleCache<K, <Engine as WasmEngine>::Module>>>;

/// Content hash of a contract container's WASM code, derived from the bytes
/// themselves rather than from the container's self-declared `code` field
/// (`ContractCode::hash()` returns a stored field; nothing recomputes it on
/// deserialization — see `ContractStore::verify_contract_identity`).
/// Returns an error rather than `unimplemented!()` on the catch-all, matching
/// `ContractStore::store_contract`'s reasoning for the identical situation:
/// unreachable today (V1 is `ContractWasmAPIVersion`'s only variant), but the
/// enum is `#[non_exhaustive]`, and this now runs on the common PUT path, so a
/// future variant would make that panic reachable from ordinary traffic.
fn wasm_code_hash(contract: &ContractContainer) -> RuntimeResult<CodeHash> {
    match contract {
        ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract_v1)) => {
            Ok(CodeHash::from_code(contract_v1.code().data()))
        }
        ContractContainer::Wasm(_) | _ => {
            Err(anyhow::anyhow!("unsupported contract container version").into())
        }
    }
}

static INSTANCE_ID: AtomicI64 = AtomicI64::new(0);

/// A live WASM instance with RAII cleanup.
///
/// On drop, removes the MEM_ADDR entry. The WASM `Instance` is cleaned
/// up by calling [`Runtime::drop_running_instance`] after the instance is
/// no longer needed.
pub(super) struct RunningInstance {
    pub id: i64,
    pub handle: InstanceHandle,
    /// Whether the contract imports `freenet_contract_io` (streaming buffer support).
    /// Contracts compiled against stdlib >= 0.3.4 have this; older ones don't.
    pub supports_streaming: bool,
    /// Set to true when the engine instance has been explicitly cleaned up.
    dropped_from_engine: bool,
}

impl RunningInstance {
    fn new(
        engine: &mut Engine,
        module: &<Engine as WasmEngine>::Module,
        key: Key,
        req_bytes: usize,
    ) -> RuntimeResult<Self> {
        let id = INSTANCE_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // Route the guest-entry call through classify_result so an epoch interrupt
        // during a runaway module start function normalizes to
        // MaxComputeTimeExceeded (Timeout class), not the generic "execution
        // timeout" that is_wasm_timeout misses (#4864 round-5).
        let handle = super::classify_result(engine.create_instance(module, id, req_bytes))?;

        // Record memory address and size for host function pointer arithmetic
        let (ptr, size) = engine.memory_info(&handle)?;
        native_api::MEM_ADDR.insert(id, InstanceInfo::new(ptr as i64, size, key));

        // Detect if the contract supports streaming buffers by checking
        // whether it imports the freenet_contract_io namespace. Contracts
        // compiled against stdlib >= 0.3.4 have this import.
        let supports_streaming = engine.module_has_streaming_io(module);

        Ok(Self {
            id,
            handle,
            supports_streaming,
            dropped_from_engine: false,
        })
    }
}

impl Drop for RunningInstance {
    fn drop(&mut self) {
        if !self.dropped_from_engine {
            tracing::debug!(
                instance_id = self.id,
                "RunningInstance dropped without engine cleanup — MEM_ADDR cleaned up, \
                 but WASM Instance will leak until engine is dropped"
            );
        }
        // Always clean up MEM_ADDR as a safety net (idempotent — engine may have already removed it)
        let _ = native_api::MEM_ADDR.remove(&self.id);
    }
}

pub(crate) struct InstanceInfo {
    pub start_ptr: i64,
    pub mem_size: usize,
    key: Key,
}

impl InstanceInfo {
    pub(crate) fn new(start_ptr: i64, mem_size: usize, key: Key) -> Self {
        Self {
            start_ptr,
            mem_size,
            key,
        }
    }

    pub fn key(&self) -> String {
        match &self.key {
            Key::Contract(k) => k.encode(),
            Key::Delegate(k) => k.encode(),
        }
    }
}

pub(super) enum Key {
    Contract(ContractInstanceId),
    Delegate(DelegateKey),
}

#[derive(thiserror::Error, Debug)]
pub enum ContractExecError {
    #[error(transparent)]
    ContractError(#[from] ContractError),

    #[error("Attempted to perform a put for an already put contract ({0}), use update instead")]
    DoublePut(ContractKey),

    #[error("could not cast array length of {0} to max size (i32::MAX)")]
    InvalidArrayLength(usize),

    #[error("unexpected result from contract interface")]
    UnexpectedResult,

    #[error(
        "The operation ran out of gas. This might be caused by an infinite loop or an inefficient computation."
    )]
    OutOfGas,

    #[error("The operation exceeded the maximum allowed compute time")]
    MaxComputeTimeExceeded,

    /// The operation never ran: it sat queued on a saturated execution pool
    /// past the wall-clock deadline and the guest never started (#4864
    /// round-6). Distinct from [`ContractExecError::MaxComputeTimeExceeded`]
    /// (a guest that DID run and blew the deadline).
    ///
    /// Classification is by TYPED PROVENANCE, NOT this string (#4864 round-9):
    /// `ExecutorError::is_scheduler_timeout` reads the `host_timeout` field that
    /// `ExecutorError::execution` sets ONLY when it sees this typed variant — which
    /// the host `classify_result` alone constructs. The message string is NOT the
    /// classification gate; it only supplies the cause text for the "execution
    /// error:" prefix and logging. Do NOT delete the `host_timeout` field and fall
    /// back to matching this phrase: a contract can RETURN a rejection whose text
    /// contains it, which would reintroduce the exact forge vector round-9 closed
    /// (a contract self-inflicting the scheduler/timeout quarantine class on honest
    /// peers). See `ExecutorError::host_timeout` in `contract/executor.rs`.
    #[error("The operation was queued too long on a saturated execution pool and never ran")]
    SchedulerOverloaded,
}

pub struct RuntimeConfig {
    /// Maximum allowed execution time for WASM code in seconds
    pub max_execution_seconds: f64,
    /// Optional override for CPU cycles per second
    pub cpu_cycles_per_second: Option<u64>,
    /// Safety margin for CPU speed variations (0.0 to 1.0)
    pub safety_margin: f64,
    pub enable_metering: bool,
    /// Byte budget for the compiled-WASM **contract** module cache. The
    /// delegate cache is sized to a fraction of this in `RuntimePool::new`
    /// (`DELEGATE_MODULE_CACHE_BUDGET_DIVISOR`). LRU entries are evicted on
    /// insert until the cache's tracked compiled-byte total is within budget.
    /// See [`default_module_cache_budget_bytes`](super::default_module_cache_budget_bytes).
    pub module_cache_budget_bytes: usize,
    /// Production opt-in to offload a cache-miss compile to a blocking thread.
    ///
    /// When `true`, `engine.compile` *may* run the Cranelift compile on a
    /// `spawn_blocking` thread so a cold-contract compile doesn't stall the
    /// current worker's other tasks (issue #4441's whole-node HANG). Whether it
    /// actually offloads is decided from the live runtime flavor inside
    /// `wasmtime_engine::compile_offloaded`: it offloads only on a MULTI-THREAD
    /// runtime and compiles inline under a current_thread / no runtime. So this
    /// flag is a safe opt-in everywhere — it can never panic and stays
    /// deterministic in the `current_thread` + `start_paused` sim runner even
    /// if set. Production sets it `true`; tests/sim leave it `false`.
    pub offload_compilation: bool,
    /// Directory for the wasmtime compile cache (#4683). `Some` relocates the
    /// cache onto the data-dir mount (so it shares the mount that sizes the disk
    /// budget and is measurable as freenet's own usage). `None` keeps wasmtime's
    /// default OS-cache location — every test / `default()` site leaves it
    /// unset, so their behavior is unchanged. An absolute path is required by
    /// wasmtime's `CacheConfig::with_directory`.
    pub wasmtime_cache_dir: Option<std::path::PathBuf>,
    /// Soft size limit (bytes) for the wasmtime compile cache (#4683). `Some`
    /// overrides wasmtime's 512 MiB default via
    /// `CacheConfig::with_files_total_size_soft_limit`; `None` keeps the default.
    /// Production resolves it from
    /// [`default_wasmtime_cache_size_bytes_for_dir`], which scales it to BOTH
    /// the memory the node may use AND the disk actually free on the cache's
    /// mount, instead of pinning a flat constant or a RAM-only figure.
    pub wasmtime_cache_size_bytes: Option<u64>,
}

/// Lower clamp for the node-relative wasmtime **on-disk compile cache** soft
/// limit (128 MiB).
///
/// # The disk-vs-CPU trade-off this floor encodes
///
/// Disk is the cheap resource here and CPU is the expensive one. Every miss is a
/// Cranelift recompile, and a node with no usable compile cache pays one for
/// every distinct contract blob it touches after each restart.
///
/// Be precise about what that costs *today*, because the pre-#4441 framing
/// ("a compile stalls the single-threaded contract loop") no longer describes
/// the code: `production_offload_compilation()` is `true` and
/// `wasmtime_engine::compile_offloaded` runs Cranelift on `spawn_blocking` under
/// `block_in_place` whenever it is on a multi-thread runtime. So other tasks on
/// the contract loop are NOT stalled. What a miss actually costs is (a) latency
/// on the requesting operation, which waits for its own compile, and (b) blocking-
/// pool pressure — a burst of cold contracts can saturate the pool and queue
/// behind itself. Real, worth avoiding, but not a whole-node stall.
///
/// # How many entries the floor actually buys
///
/// The cache is keyed per **distinct WASM blob** (engine config + bytes hash),
/// NOT per contract instance, so contracts sharing code — every River room shares
/// one room-contract blob — share a single entry. A node hosting hundreds of
/// contracts does not need hundreds of entries.
///
/// Measured directly on the production gateway (nova,
/// `~freenet/.local/share/freenet/wasmtime-cache`, 2026-07-28): **418 artifacts,
/// 198.8 MiB total, mean 487 KiB, p50 397 KiB, p90 811 KiB, max 1.70 MiB**,
/// against **185 live `*.wasm` blobs** in the contracts dir (entries outlive blob
/// deletion and span engine-config changes, hence entries > live blobs). At that
/// p90, 128 MiB holds ~161 artifacts; at the mean, ~269.
///
/// Deliberately NOT cited here:
/// `wasm_runtime::tests::cache::test_compiled_module_size_is_in_expected_range`.
/// That measures the **in-memory** `Module::serialize()` size of one trivial
/// fixture with a very wide tolerance band — a different and larger quantity than
/// a zstd-compressed on-disk entry, so it cannot support a claim about how many
/// entries fit on disk.
///
/// # Why the floor is not lower
///
/// It binds hardest exactly where the recompile cost is worst: a small host also
/// gets the smallest **in-memory** module cache
/// (`MIN_DEFAULT_MODULE_CACHE_BUDGET_BYTES`, 64 MiB), so it evicts compiled
/// modules from RAM more often and leans on this on-disk cache *more* than a
/// large host does. It is set equal to the hosting budget's own floor
/// (`MIN_DEFAULT_HOSTING_BUDGET_BYTES`, also 128 MiB) so the two on-disk
/// allowances stay consistent: the smallest node the code contemplates gets the
/// same floor for the state it hosts and for the compiled code that serves it.
pub(crate) const MIN_WASMTIME_CACHE_SIZE_BYTES: u64 = 128 * 1024 * 1024;

/// Upper clamp for the node-relative wasmtime on-disk compile cache soft limit
/// (512 MiB).
///
/// Equal to the historical flat value (wasmtime's own default, which #4683
/// pinned explicitly) ON PURPOSE, mirroring the rationale on
/// `MAX_DEFAULT_HOSTING_BUDGET_BYTES`: a host with ample memory (>= 4 GiB at the
/// current divisor) resolves to exactly the previous 512 MiB and sees NO change.
/// That makes this a pure "small and containerized nodes get less" change rather
/// than a cache *increase* for anyone — a production gateway keeps the ceiling
/// it has been running with, so nothing about the large-host recompile profile
/// moves and the change carries no new disk commitment to validate in the field.
pub(crate) const MAX_WASMTIME_CACHE_SIZE_BYTES: u64 = 512 * 1024 * 1024;

/// Fraction of the memory the node may use that sizes the on-disk compile
/// cache: 1/8.
///
/// The same divisor the node already applies to its hosted-state budget
/// (`ring::hosting::cache::DEFAULT_HOSTING_BUDGET_RAM_DIVISOR`) and to its
/// in-memory module cache (`module_cache::DEFAULT_MODULE_CACHE_RAM_DIVISOR`), so
/// the node keeps ONE story for how it splits its capability into budgets
/// instead of three unrelated fractions.
///
/// Why a memory signal sizes a *disk* cache: the useful size of this cache is
/// set by how many distinct contract blobs the node executes, and that population
/// is what the RAM-scaled hosting budget already bounds. Sharing the divisor
/// makes the relationship exact instead of coincidental — with the same divisor,
/// the same floor, and a strictly lower ceiling, the compile-cache **default**
/// can never exceed the hosted-state **default** for the same host (pinned by
/// `compile_cache_default_never_exceeds_hosting_default`). That is the shape of
/// the defect this replaced: on a peer under a 2 GiB cgroup the flat 512 MiB
/// limit let the compile cache reach ~306 MB while that node's entire
/// contract-state budget was 256 MiB.
///
/// # That relationship holds between DEFAULTS, not between live budgets
///
/// The hosted-state budget is operator-overridable (`--max-hosting-storage` /
/// `MAX_HOSTING_STORAGE`, `config.rs:120`, resolved at `config.rs:1054`), and the
/// compile-cache limit is not overridable at all. So an operator who sets
/// `--max-hosting-storage 64MiB` on a 4 GiB box gets a 64 MiB state budget beside
/// a 512 MiB compile cache, and the "never exceeds" property does NOT hold for
/// that node. What the shared divisor guarantees is only that the two *derived
/// defaults* stay ordered at every host size. Do not restate this as a
/// system-level invariant.
///
/// Note also that a RAM signal ALONE is not the right shape for this cache's
/// real constraint: the compile cache is charged against the aggregate
/// **disk** budget (`DiskUsageTracker::total_bytes()` sums state + wasm +
/// compile-cache bytes and gates `admit_state_write` / `admit_wasm_write`), so
/// a disk-tight but RAM-rich host is not protected by a RAM-derived bound on
/// its own. This divisor narrows the exposure on RAM-poor hosts; the disk-tight
/// case is closed by composing this RAM term with a disk-derived term via
/// `min()` — see [`combine_wasmtime_cache_size`] / [`wasmtime_cache_size_for_disk`]
/// (#5014).
const WASMTIME_CACHE_RAM_DIVISOR: u64 = 8;

/// Fallback "memory the node may use" estimate (1 GiB) when the OS query fails.
///
/// Mirrors the module cache's and the hosting budget's fallback. At 1 GiB the
/// divisor lands exactly on [`MIN_WASMTIME_CACHE_SIZE_BYTES`], so a host whose
/// capability we cannot read gets the smallest sane cache rather than the
/// largest.
const WASMTIME_CACHE_FALLBACK_TOTAL_RAM_BYTES: u64 = 1024 * 1024 * 1024;

/// Default soft size limit for the wasmtime **on-disk compile cache**, scaled to
/// the memory the node may use (host RAM, or a smaller cgroup limit when
/// containerized — see [`read_total_ram_bytes`](super::read_total_ram_bytes))
/// and clamped to a sane floor/ceiling.
///
/// Returns `clamp(total_ram / WASMTIME_CACHE_RAM_DIVISOR,
/// MIN_WASMTIME_CACHE_SIZE_BYTES, MAX_WASMTIME_CACHE_SIZE_BYTES)` — currently
/// `clamp(total_ram / 8, 128 MiB, 512 MiB)`. It replaces a flat 512 MiB constant
/// that applied regardless of machine size, which let a 2 GiB-cgroup node keep a
/// compile cache larger than its whole 256 MiB contract-state budget.
///
/// # This is a SOFT limit, and the steady state is 70% of it
///
/// Wasmtime prunes on its ~1h cleanup, and it deletes down to
/// `soft_limit × files_total_size_limit_percent_if_deleting / 100`. That percent
/// defaults to **70** (`wasmtime-internal-cache/src/config.rs:219`, applied at
/// `worker.rs:479-485`). Two consequences the arithmetic must not gloss over:
///
/// - The steady-state footprint after a prune is `soft_limit × 0.7`, not
///   `soft_limit`.
/// - Between cleanups the cache may legitimately sit at the FULL soft limit, so
///   the disk it is charged against must tolerate the un-pruned figure, not just
///   the steady-state one.
///
/// # Margin at the smallest shape is thin, not comfortable
///
/// At the 2 GiB-cgroup shape the 256 MiB limit steady-states to ~179 MiB. For
/// scale: the production gateway's measured working set is 198.8 MiB across 418
/// artifacts (see [`MIN_WASMTIME_CACHE_SIZE_BYTES`]) — larger than that steady
/// state. A 2 GiB node hosts far less than that gateway, so it is not the same
/// working set, but the honest statement is that this shape has roughly zero
/// headroom rather than room to spare: a 2 GiB node whose working set grows past
/// ~179 MiB will prune and recompile on the margin. That is the trade being made
/// deliberately — the alternative was a cache bigger than the node's entire state
/// budget.
///
/// # Which cache this is
///
/// The **on-disk** cache of compiled artifacts wasmtime writes under the data
/// dir. It is NOT the in-memory compiled-module LRU
/// ([`ModuleCache`](super::ModuleCache), sized by
/// [`default_module_cache_budget_bytes`](super::default_module_cache_budget_bytes)
/// and overridable via `--module-cache-budget-bytes`). The two are separate
/// caches with separate budgets; this one has no operator override today (it
/// never had one — it was a private constant), so this derived default (see
/// [`default_wasmtime_cache_size_bytes_for_dir`]) is its only source.
///
/// # Pure clamp math behind the RAM-side term
///
/// Split out so the small-box / large-box / cgroup boundary behavior is
/// unit-testable without depending on the test host's real RAM. Mirrors the
/// `budget_for_ram` / `disk_budget_for_clamped` pattern used by the sibling
/// budgets.
pub(crate) fn wasmtime_cache_size_for_ram(total_ram: u64) -> u64 {
    (total_ram / WASMTIME_CACHE_RAM_DIVISOR)
        .clamp(MIN_WASMTIME_CACHE_SIZE_BYTES, MAX_WASMTIME_CACHE_SIZE_BYTES)
}

/// Fraction of the disk space *available* on the compile-cache's mount that
/// sizes the on-disk compile cache's disk-side term (#5014): 1/8, the SAME
/// fraction [`WASMTIME_CACHE_RAM_DIVISOR`] applies on the RAM side, so this
/// stays one story ("an eighth of the resource, floored and ceilinged the
/// same way") rather than two unrelated fractions.
const WASMTIME_CACHE_DISK_DIVISOR: u64 = 8;

/// Lower clamp for the compile cache's DISK-side term ONLY (#5328 review) —
/// deliberately LOWER than [`MIN_WASMTIME_CACHE_SIZE_BYTES`] (the RAM-side
/// floor, also the aggregate hosting-disk budget's own floor,
/// `MIN_DEFAULT_HOSTING_BUDGET_BYTES` in `ring/hosting/cache.rs`).
///
/// If the disk-side term shared the 128 MiB RAM-side floor, the two floors
/// would COLLIDE on a genuinely small disk: `wasmtime_cache_size_for_disk`
/// would floor at 128 MiB at the exact same reachable-disk size
/// (256 MiB) where the aggregate disk budget ALSO floors at 128 MiB,
/// leaving EXACTLY ZERO headroom for actual contract state — the compile
/// cache alone would consume the entire disk-budget floor, on any host with
/// <= 256 MiB reachable disk. That is a narrower, but still real, residual
/// instance of the wedge #5014 exists to fix.
///
/// Set to `MIN_WASMTIME_CACHE_SIZE_BYTES / 4` (32 MiB) so the two floors'
/// breakeven points coincide exactly (`32 MiB * WASMTIME_CACHE_DISK_DIVISOR
/// == 256 MiB == MIN_DEFAULT_HOSTING_BUDGET_BYTES /
/// DEFAULT_HOSTING_DISK_PCT`), which makes the headroom function
/// `disk_budget - disk_term` CONTINUOUS and strictly positive across every
/// reachable-disk size, not just the one worked example in the issue —
/// verified by `disk_term_never_exceeds_a_quarter_of_the_aggregate_disk_budget_floor`
/// and `headroom_is_always_positive_across_reachable_disk_sizes` below. 32 MiB
/// still buys ~40 entries at the measured p90 on-disk artifact size (811 KiB,
/// see [`MIN_WASMTIME_CACHE_SIZE_BYTES`]'s doc) — reduced from the RAM
/// floor's ~161, but a host this disk-constrained also hosts far fewer
/// distinct contracts, so a smaller working set is the right trade rather
/// than zero state headroom.
const MIN_WASMTIME_CACHE_SIZE_BYTES_FOR_DISK: u64 = MIN_WASMTIME_CACHE_SIZE_BYTES / 4;

/// Pure clamp math for the compile cache's DISK-side term (#5014), the disk
/// analogue of [`wasmtime_cache_size_for_ram`]. Same ceiling
/// (`MAX_WASMTIME_CACHE_SIZE_BYTES`) as the RAM side but its OWN, lower floor
/// ([`MIN_WASMTIME_CACHE_SIZE_BYTES_FOR_DISK`] — see that constant's doc for
/// why sharing the RAM-side floor would leave zero state-budget headroom on
/// a small disk).
///
/// `available_disk_bytes` is deliberately just the free-space reading, NOT
/// `used + available` the way [`crate::ring::hosting::cache::disk_budget_for_clamped`]
/// sizes the aggregate hosting-disk budget: that basis exists so the OVERALL
/// budget doesn't shrink as a node fills with its OWN legitimately-admitted
/// state. Here we want the opposite bias — a fresh `statvfs` read of "what's
/// free right now" is the more conservative (safer) signal for a cache that
/// is about to compete with state/wasm writes for that same headroom, and it
/// needs no pre-seeded "used" figure, which isn't available yet at the point
/// in startup this sizing runs (see [`default_wasmtime_cache_size_bytes_for_dir`]).
/// The caller is responsible for making `available_disk_bytes` itself stable
/// across restarts (folding the cache's OWN current footprint back in) — see
/// that function's "Why `available_disk_bytes` folds the cache's own size
/// back in" section; this function only applies the clamp.
pub(crate) fn wasmtime_cache_size_for_disk(available_disk_bytes: u64) -> u64 {
    (available_disk_bytes / WASMTIME_CACHE_DISK_DIVISOR).clamp(
        MIN_WASMTIME_CACHE_SIZE_BYTES_FOR_DISK,
        MAX_WASMTIME_CACHE_SIZE_BYTES,
    )
}

/// Combine the RAM-side and disk-side terms into the compile cache's actual
/// soft limit (#5014): `min(ram_term, disk_term)`, so a disk-tight host is
/// bounded even when RAM is ample. `available_disk_bytes` is `None` when the
/// mount's free-space signal could not be read (statvfs failure or an
/// unsupported platform) — mirrors
/// [`crate::ring::hosting::disk_usage::available_bytes`]'s own rule that an
/// unreadable signal must not silently shrink a budget: fall back to the
/// RAM-only figure (today's shipped behavior) rather than invent a possibly-
/// wrong tight cap from a signal we don't trust.
///
/// Split out from [`default_wasmtime_cache_size_bytes_for_dir`] as pure
/// function so the RAM/disk interaction (which one binds, the `None`
/// fallback) is unit-testable without a real host's RAM or a real mount.
pub(crate) fn combine_wasmtime_cache_size(
    total_ram: u64,
    available_disk_bytes: Option<u64>,
) -> u64 {
    let ram_term = wasmtime_cache_size_for_ram(total_ram);
    match available_disk_bytes {
        Some(available) => ram_term.min(wasmtime_cache_size_for_disk(available)),
        None => ram_term,
    }
}

/// The wasmtime on-disk compile cache's soft limit, bounded by BOTH the
/// memory the node may use AND the disk space actually free on the cache
/// directory's mount (#5014). This is the function the production path
/// (`Executor::from_config_with_shared_modules`) calls; the non-production
/// `Runtime::build` path never relocates the cache and has no directory to
/// probe, so it keeps wasmtime's own default location + flat 512 MiB limit,
/// unchanged.
///
/// # Callers MUST gate this on actually building a new backend engine
///
/// This does real filesystem work (a `statvfs` call and, via
/// [`reconcile_existing_cache_dir`], a full recursive directory walk and
/// possibly a `remove_dir_all`), so a caller must call it only for the ONE
/// executor per node that actually builds a fresh `wasmtime::Engine` /
/// `Cache` (`shared_backend.is_none()` in `from_config_with_shared_modules`
/// — every other pool worker, and every mid-life
/// `create_replacement_executor` panic-recovery call, reuses the already-built
/// shared engine and never reads the value this returns). Calling it
/// unconditionally would, on a live node, run the reconciliation's
/// `remove_dir_all` against a directory the shared engine is actively
/// reading/writing — exactly the kind of already-populated, in-use cache the
/// reconciliation is meant to only ever touch once, at boot, before anything
/// is using it (#5328 review).
///
/// # Why this needs the directory, not just a number
///
/// Wasmtime applies its soft limit once, at `Cache::new`, with no live
/// re-application path — so this is a start-time-only value, and the
/// filesystem probe (`statvfs` on `dir`) has to happen HERE, synchronously,
/// before the engine is built. It cannot go through the aggregate
/// [`crate::ring::hosting::disk_usage::DiskUsageTracker`] instead: that
/// tracker is seeded lazily on the first ~60s sweep tick, well after this
/// function's caller needs an answer, and seeding it here would mean walking
/// every persisted contract-state row before the node can even build its WASM
/// engine.
///
/// # Immediate relief for an already-oversized cache
///
/// A node upgrading from an older build (or one that just got less disk) can
/// already have MORE on disk than the limit computed here. Wasmtime's own
/// cleanup is gated by a marker file compared against a ~1h interval, and
/// that marker persists across restarts — so a node that hasn't cleaned up
/// recently often DOES prune promptly on its first post-restart cache write
/// (#5328 review), not after a fixed wait. But there is no GUARANTEE of
/// that (a node that restarts often, or restarts shortly after its own
/// cleanup ran, waits out the rest of the interval either way), and while
/// waiting the wedge persists. Reconciling here, once, at startup, gives
/// the fix effect on the very next restart unconditionally, rather than
/// depending on wasmtime's internal cleanup timing (see
/// [`reconcile_existing_cache_dir`]).
///
/// # Why `available_disk_bytes` folds the cache's own current size back in
///
/// A naive `statvfs` read of raw free space makes the computed limit a
/// function of the cache's OWN current footprint — the cache occupies disk,
/// so a bigger cache means less "available," means a SMALLER computed limit
/// next boot, which (if it now reads as an overshoot) triggers
/// [`reconcile_existing_cache_dir`] to wipe the cache, which makes MORE disk
/// "available" next boot, computing a LARGER limit, letting the cache regrow
/// toward it, shrinking "available" again on the boot after that — a
/// feedback loop that (#5328 review, verified by hand) converges to
/// wiping-every-other-restart in steady state for an actively-used node,
/// defeating the entire point of a persistent on-disk compile cache. Adding
/// the cache's OWN current on-disk size back to the raw `statvfs` reading
/// makes the basis `raw_free + current_cache_bytes` — the disk capacity
/// reachable if the cache were empty — which does NOT depend on the cache's
/// current size, so the computed limit is STABLE across restarts as long as
/// other disk usage (state, wasm, unrelated files) doesn't change. This
/// mirrors the SAME `used + available` stability rationale
/// [`crate::ring::hosting::cache::disk_budget_for_clamped`] already documents
/// for the aggregate hosting-disk budget.
///
/// # Why the operator's configured disk budget also has to bind
///
/// Bounding purely by PHYSICAL disk availability closes the accidental case
/// (a small physical disk) but not the deliberate one: an operator who sets
/// `--max-hosting-disk` below the disk's physical capacity (e.g. to reserve
/// room for other services on a large shared disk) still has physical
/// availability read as large, so the physical term alone would still
/// resolve to the RAM ceiling — reproducing #5014's wedge against the
/// operator's OWN configured budget instead of against physical scarcity.
/// This was the ORIGINAL issue's own suggested direction (reuse
/// [`crate::ring::hosting::cache::disk_budget_for_clamped`], the exact
/// function the live aggregate budget uses), not an addition beyond its
/// scope. See [`bound_by_configured_disk_budget`].
pub(crate) fn default_wasmtime_cache_size_bytes_for_dir(
    dir: &Path,
    hosting_disk_pct: f64,
    max_hosting_disk: u64,
) -> u64 {
    let total_ram = super::read_total_ram_bytes()
        .map(|v| v as u64)
        .unwrap_or(WASMTIME_CACHE_FALLBACK_TOTAL_RAM_BYTES);
    // Walk ONCE, share the result between the stabilized availability signal
    // and the reconciliation threshold check below — no need to re-walk.
    let current_cache_bytes = crate::ring::disk_directory_size_bytes(dir);
    let raw_available_disk_bytes = crate::ring::disk_available_bytes(dir);
    let stabilized_available_disk_bytes =
        stabilize_available_disk_bytes(raw_available_disk_bytes, current_cache_bytes);
    let physical_term = combine_wasmtime_cache_size(total_ram, stabilized_available_disk_bytes);
    let limit = bound_by_configured_disk_budget(
        physical_term,
        current_cache_bytes,
        raw_available_disk_bytes,
        hosting_disk_pct,
        max_hosting_disk,
    );
    reconcile_existing_cache_dir(dir, current_cache_bytes, limit);
    limit
}

/// Fraction of the operator's CONFIGURED aggregate disk budget the compile
/// cache may consume on its own (#5328 review) — 1/4, the SAME fraction
/// [`MIN_WASMTIME_CACHE_SIZE_BYTES_FOR_DISK`] uses relative to the RAM-side
/// floor, so the two mechanisms agree at their shared worst case: when the
/// configured budget is itself at ITS OWN floor
/// (`MIN_DEFAULT_HOSTING_BUDGET_BYTES` = 128 MiB), a quarter of it is exactly
/// 32 MiB — [`MIN_WASMTIME_CACHE_SIZE_BYTES_FOR_DISK`]'s own value — so this
/// term never re-opens the floor-collision headroom gap that constant was
/// added to close.
const CONFIGURED_DISK_BUDGET_ALLOWANCE_DIVISOR: u64 = 4;

/// Further bound `physical_term` (already sized from RAM + raw physical disk)
/// by a fraction of what the AGGREGATE hosting-disk budget will project to,
/// using the operator's configured `hosting_disk_pct` / `max_hosting_disk`
/// (#5328 review — see [`default_wasmtime_cache_size_bytes_for_dir`]'s "Why
/// the operator's configured disk budget also has to bind"). Computed via
/// the SAME [`crate::ring::hosting::cache::disk_budget_for_clamped`] function
/// the live aggregate budget uses, fed `(used = current_cache_bytes,
/// available = raw_available_disk_bytes)` — the same `used + available`
/// identity [`stabilize_available_disk_bytes`] already relies on, so this
/// projection is STABLE across restarts for the same reason that function
/// is (a pure function of total reachable capacity, not of how much the
/// cache itself currently occupies).
///
/// `raw_available_disk_bytes: None` (unreadable signal) skips this bound
/// entirely — the physical term's own `None`-fallback already applies, and
/// there is no available/used basis to project a budget from.
fn bound_by_configured_disk_budget(
    physical_term: u64,
    current_cache_bytes: u64,
    raw_available_disk_bytes: Option<u64>,
    hosting_disk_pct: f64,
    max_hosting_disk: u64,
) -> u64 {
    let Some(raw_available) = raw_available_disk_bytes else {
        return physical_term;
    };
    let configured_budget = crate::ring::disk_budget_for_clamped(
        current_cache_bytes,
        raw_available,
        hosting_disk_pct,
        crate::ring::MIN_DEFAULT_HOSTING_BUDGET_BYTES,
        max_hosting_disk,
    );
    physical_term.min(configured_budget / CONFIGURED_DISK_BUDGET_ALLOWANCE_DIVISOR)
}

/// Fold a directory's own current on-disk size back into a raw free-space
/// reading, so the result represents "disk reachable if this directory were
/// empty" rather than "disk free right now" — see
/// [`default_wasmtime_cache_size_bytes_for_dir`]'s "Why `available_disk_bytes`
/// folds the cache's own current size back in" doc for the boot-to-boot
/// oscillation this prevents (#5328 review). `None` (unreadable raw signal)
/// stays `None` — folding must never turn an untrusted signal into a trusted
/// one.
///
/// Split out as a pure function so the stabilization property is testable
/// deterministically: a real end-to-end test through actual `statvfs` cannot
/// reliably distinguish "fixed" from "buggy" on a host with generous free
/// disk (the oscillation only manifests when the cache's own footprint is a
/// non-negligible fraction of `available` — a real dev/CI machine's disk is
/// typically hundreds of GB free, dwarfing even a full 512 MiB cache, so both
/// versions land on the same ceiling-clamped answer and the test is vacuous).
fn stabilize_available_disk_bytes(
    raw_available: Option<u64>,
    current_cache_bytes: u64,
) -> Option<u64> {
    raw_available.map(|raw| raw.saturating_add(current_cache_bytes))
}

/// If a PRIOR run already left more than `new_soft_limit_bytes` on disk under
/// `dir` — a RAM-rich host whose disk tightened, an operator who moved to a
/// smaller disk, or simply a node upgrading from before #5014 narrowed this
/// limit — clear the directory rather than waiting on wasmtime's own ~1h
/// internal prune cycle to catch up. `current_bytes` is the caller's ALREADY
/// walked measurement (see [`default_wasmtime_cache_size_bytes_for_dir`]) —
/// this function does no filesystem read of its own beyond the delete.
///
/// This is a pure cache of recompiled-from-WASM artifacts: clearing it is
/// always safe (worst case, the next distinct contract blob recompiles once
/// instead of hitting the cache) and is the only way to give an
/// ALREADY-WEDGED node (#5014: the aggregate disk budget's `admit_state_write`
/// / `admit_wasm_write` rejecting every write because the disk budget counts
/// the oversized cache) relief on the very next restart, instead of an
/// up-to-an-hour wait.
///
/// Best-effort: this is a startup optimization, not a correctness
/// requirement, so a delete failure is logged and otherwise ignored — it must
/// never fail node boot, and a directory that fails to clear just falls back
/// to wasmtime's own prune cycle, the pre-#5014 behavior.
fn reconcile_existing_cache_dir(dir: &Path, current_bytes: u64, new_soft_limit_bytes: u64) {
    if current_bytes <= new_soft_limit_bytes {
        return;
    }
    tracing::info!(
        dir = %dir.display(),
        current_bytes,
        new_soft_limit_bytes,
        "wasmtime compile cache exceeds the newly-computed disk-aware soft \
         limit; clearing it for immediate relief (#5014)"
    );
    if let Err(error) = std::fs::remove_dir_all(dir) {
        tracing::warn!(
            dir = %dir.display(),
            %error,
            "failed to clear oversized wasmtime compile cache; falling back \
             to wasmtime's own prune cycle"
        );
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_execution_seconds: 5.0,
            cpu_cycles_per_second: None,
            safety_margin: 0.2,
            enable_metering: false,
            module_cache_budget_bytes: super::default_module_cache_budget_bytes(),
            // Default off so that any code path building a `RuntimeConfig`
            // without explicitly opting in (tests, sim) keeps the deterministic
            // inline compile. Production opts in explicitly — see
            // `RuntimePool::new` / `from_config_with_shared_modules`.
            offload_compilation: false,
            // Default: keep wasmtime's own OS-cache location + 512 MiB soft
            // limit. Only the production `from_config*` path relocates + sizes
            // it, so tests and sims see unchanged wasmtime cache behavior.
            wasmtime_cache_dir: None,
            wasmtime_cache_size_bytes: None,
        }
    }
}

/// Callback invoked after a successful state write from a V2 delegate host
/// function (`put_contract_state_sync` or `update_contract_state_sync`).
///
/// V2 delegate writes go through `db.store_state_sync` / `db.update_state_sync`
/// directly and bypass the executor's `state_store.{store,update}` chokepoints
/// where the bump+refresh+report sites live. Without this callback those
/// three side effects never fire on a V2 delegate write, leaving the
/// EvictContract re-host race open AND undercounting StateBytesWritten in
/// the topology meter for that path. The wiring lives outside `wasm_runtime/`
/// (Ring lives in `crates/core/src/ring.rs`) so the callback is plumbed via
/// a trait object owned by `Runtime` to keep `wasm_runtime` independent of
/// the ring.
///
/// The closure SHOULD delegate to `Ring::commit_state_write(key, state_size)`
/// — see `RuntimePool::contract_state_write_callback` for the production
/// wiring. The `state_size` argument is the on-disk byte count of the
/// newly-written state and is fed into the StateBytesWritten meter axis
/// for governance scoring.
pub type StateWriteCallback =
    Arc<dyn Fn(&freenet_stdlib::prelude::ContractKey, usize) + Send + Sync + 'static>;

/// Pre-write admission gate for V2 delegate state writes (#4683, PR 3).
///
/// V2 `put_contract_state_sync` / `update_contract_state_sync` bypass the
/// executor's `state_store.{store,update}` chokepoints, so the disk-budget
/// admission gate the executor applies there does not run for them. This
/// callback restores it: invoked with `(key, new_state_size)` BEFORE the raw
/// `Storage` write, it returns `Err(cause)` when the write would push aggregate
/// disk past the budget, and the native-API method aborts without writing (no
/// rollback needed — nothing landed). The wiring lives outside `wasm_runtime/`
/// (Ring is in `crates/core/src/ring.rs`), so — like [`StateWriteCallback`] —
/// it is plumbed via a trait object to keep `wasm_runtime` ring-independent.
/// The `Err` payload is a human-readable cause string surfaced to the delegate
/// caller.
///
/// The `is_update` flag selects the admission semantics (#4683): `false` for a
/// V2 PUT (`put_contract_state_sync`) applies the HARD gate (any write that
/// would push the aggregate over budget is rejected); `true` for a V2 UPDATE
/// (`update_contract_state_sync`) applies the GROWTH-ONLY gate (a shrinking or
/// size-holding write is always admitted, even over budget, so a CRDT merge
/// never blocks convergence — only genuine growth is bounded). This mirrors the
/// executor-side split between `admit_state_write` (PUT) and
/// `admit_state_update` (UPDATE / re-PUT merge).
pub type StateAdmitCallback = Arc<
    dyn Fn(&freenet_stdlib::prelude::ContractKey, usize, bool) -> Result<(), String>
        + Send
        + Sync
        + 'static,
>;

pub struct Runtime {
    /// The WASM engine backend (wasmtime).
    pub(super) engine: Engine,

    pub(super) secret_store: SecretsStore,
    pub(super) delegate_store: DelegateStore,
    /// LRU cache of compiled delegate modules (shared across pool executors),
    /// keyed by the CODE hash rather than the delegate key — see
    /// [`SharedModuleCache`] and `prepare_delegate_call`.
    pub(super) delegate_modules: SharedModuleCache<CodeHash>,
    /// Persisted `ctx.write()` bytes per delegate, shared across pool
    /// executors so a prompt round-trip routed to a different `Runtime` still
    /// sees the pending state. See `native_api::DelegateContextCache`.
    pub(super) delegate_contexts: super::native_api::DelegateContextCache,
    /// This node's count of delegates created via the `create_delegate` host
    /// function, enforcing `MAX_CREATED_DELEGATES_PER_NODE`. Shared across the
    /// pool's executors (so the limit is per node, not per executor) and NOT
    /// across nodes. See `native_api::SharedDelegateCounter`.
    pub(crate) created_delegates_count: super::native_api::SharedDelegateCounter,
    /// This node's child-delegate attestation map (child `DelegateKey` → the
    /// origins it inherited from its parent). Shared across the pool's
    /// executors and NOT across nodes — it is an authorization input, see
    /// `native_api::SharedInheritedOrigins`.
    pub(crate) inherited_origins: super::native_api::SharedInheritedOrigins,

    /// Local contract storage.
    pub(crate) contract_store: ContractStore,
    /// LRU cache of compiled contract modules (shared across pool executors),
    /// keyed by the CODE hash rather than the contract instance — see
    /// [`SharedModuleCache`] and `prepare_contract_call_inner`.
    pub(super) contract_modules: SharedModuleCache<CodeHash>,

    /// Optional state storage backend for V2 delegate contract access.
    pub(crate) state_store_db: Option<crate::contract::storages::Storage>,

    /// Optional callback invoked after a successful V2 delegate state write,
    /// used to bump the per-contract generation token and refresh the
    /// hosting-cache snapshot from the V2 path (which bypasses the executor
    /// chokepoints). See `StateWriteCallback`.
    pub(crate) state_write_callback: Option<StateWriteCallback>,

    /// Optional pre-write disk-budget admission gate for V2 delegate state
    /// writes (#4683, PR 3). Installed alongside `state_write_callback`; when
    /// present it runs BEFORE the raw `Storage` write and can abort it. See
    /// [`StateAdmitCallback`].
    pub(crate) state_admit_callback: Option<StateAdmitCallback>,
}

impl Runtime {
    /// Check if the runtime is in a healthy state and can execute WASM.
    pub fn is_healthy(&self) -> bool {
        self.engine.is_healthy()
    }

    /// Get a clone of the backend engine for sharing with other runtimes.
    pub(crate) fn clone_backend_engine(&self) -> BackendEngine {
        self.engine.clone_backend_engine()
    }

    /// Set the state storage backend for V2 delegate contract access.
    pub fn set_state_store_db(&mut self, db: crate::contract::storages::Storage) {
        self.state_store_db = Some(db);
    }

    /// Install a callback invoked after each successful V2 delegate state
    /// write. See `StateWriteCallback`. Without this, V2 PUT/UPDATE bypass
    /// the executor's bump+refresh chokepoints and the EvictContract
    /// re-host race stays open for V2 delegate writes.
    pub fn set_state_write_callback(&mut self, cb: StateWriteCallback) {
        self.state_write_callback = Some(cb);
    }

    /// Install a pre-write disk-budget admission gate for V2 delegate state
    /// writes (#4683, PR 3). See [`StateAdmitCallback`]. Without it, V2
    /// PUT/UPDATE bypass the executor's admission gate and can overflow the
    /// aggregate disk budget.
    pub fn set_state_admit_callback(&mut self, cb: StateAdmitCallback) {
        self.state_admit_callback = Some(cb);
    }

    /// Export every secret under `scope` from this runtime's secrets store into
    /// an encrypted [`super::secret_export`] bundle (the live counterpart of the
    /// offline `freenet secrets export` CLI). The bundle is sealed under
    /// `material` so the user can later re-import it with the same key.
    ///
    /// This is the ONLY route to the `pub(super) secret_store` from outside the
    /// `wasm_runtime` module: the executor (`contract::executor`) lives in a
    /// different module tree and cannot touch the field directly, so it wraps
    /// secret access in `Runtime` methods exactly as `register_delegate` /
    /// `inbound_app_message` do. Used by the hosted-mode export endpoint
    /// (P3-live of #4381) to export a single hosted user's per-user secrets.
    ///
    /// Plaintext exists only in the `Zeroizing` buffers inside `export_bundle`;
    /// the returned bytes are encrypted at rest.
    ///
    /// PERFORMANCE / DoS (#4381 P5, addressed): this enumerates AND
    /// AEAD-decrypts EVERY secret in `scope`, synchronously. Two guards keep an
    /// authenticated token-holder from wedging the node with it:
    /// - **Per-user bound**: `export_bundle` rejects (before the heavy work) an
    ///   export exceeding `MAX_EXPORT_SECRET_COUNT` /
    ///   `MAX_EXPORT_TOTAL_PLAINTEXT_BYTES`, bounding the worst-case work.
    /// - **Off-loop execution**: the hosted-export caller
    ///   (`RuntimePool::export_user_secrets`) runs this on a blocking thread
    ///   (`spawn_blocking`, runtime-flavor-gated), so it does NOT stall the
    ///   single-threaded contract-handling loop while it runs.
    ///
    /// Broader per-user rate/quota limiting (repeated exports over time) remains
    /// part of the wider P5 abuse work tracked under #4381.
    pub(crate) fn export_secret_bundle(
        &self,
        scope: super::secrets_store::SecretScope<'_>,
        material: &super::secret_export::BundleKeyMaterial<'_>,
    ) -> Result<Vec<u8>, super::secret_export::ExportError> {
        super::secret_export::export_bundle(&self.secret_store, scope, material)
    }

    /// Import secrets from an encrypted [`super::secret_export`] bundle into this
    /// runtime's secrets store at `target_scope` (the live counterpart of the
    /// offline `freenet secrets import` CLI — but without stopping the node,
    /// P3-live of #4592).
    ///
    /// The MUTATING analogue of [`Self::export_secret_bundle`], and the ONLY
    /// route to the `pub(super) secret_store` for a write from outside the
    /// `wasm_runtime` module (the executor lives in a different module tree and
    /// cannot touch the field directly, so it wraps secret access in `Runtime`
    /// methods exactly as `register_delegate` / `export_secret_bundle` do).
    ///
    /// All-or-nothing on the KEY: [`super::secret_export::import_bundle`] calls
    /// `open_bundle` (which decrypts the WHOLE bundle and authenticates it)
    /// BEFORE any write, so a wrong key / corrupt bundle returns an error with
    /// NOTHING written. Plaintext exists only in the `Zeroizing` buffers inside
    /// `import_bundle`; the re-encrypted-at-rest blobs are written under this
    /// node's per-delegate DEK.
    ///
    /// PERFORMANCE: this decrypts every entry AND writes each to disk (one ReDb
    /// index update + one file per secret), synchronously. The live-import caller
    /// (`RuntimePool::import_secrets`) runs it ON the contract loop (serialized
    /// with delegate `store_secret`) — DELIBERATELY on-loop, because the import
    /// WRITES and the store write path assumes node-wide write serialization
    /// (running it off-loop would let it race another writer on the same secret
    /// file). The loop-block is acceptable: the endpoint is loopback +
    /// dashboard-gated (a one-shot operator migration), not the authenticated-
    /// remote DoS surface that justified moving the read-only EXPORT off-loop.
    pub(crate) fn import_secret_bundle(
        &mut self,
        bundle: &[u8],
        material: &super::secret_export::BundleKeyMaterial<'_>,
        target_scope: &super::secret_export::TargetScope,
        overwrite: bool,
    ) -> Result<super::secret_export::ImportReport, super::secret_export::ExportError> {
        super::secret_export::import_bundle(
            &mut self.secret_store,
            bundle,
            material,
            target_scope,
            overwrite,
        )
    }

    /// One-shot, idempotent, Local-scope copy-forward of delegate secrets from
    /// `predecessors` into `successor` (#4117), the node-side primitive behind
    /// `DelegateRequest::RegisterDelegateWithPredecessors`. Another route to the
    /// `pub(super) secret_store` for a write from outside the `wasm_runtime`
    /// module (the executor lives in a different module tree and wraps secret
    /// access in `Runtime` methods, exactly as `register_delegate` /
    /// `import_secret_bundle` do).
    ///
    /// Never returns an error: every failure is recorded in the returned
    /// [`super::MigrationReport`] and logged, so a registration is never blocked
    /// by a predecessor's data being absent or partly unreadable. See
    /// [`SecretsStore::migrate_secrets`] for the full contract (Local-only DEK
    /// carve-out, no-delete invariant, one-shot / anti-resurrection marker).
    ///
    /// Runs ON the contract loop (serialized with delegate `store_secret`),
    /// mirroring the on-loop write discipline of `import_secret_bundle`.
    ///
    /// UNREACHABLE as of GHSA-824h-7x5x-wfmf: the sole caller (the
    /// `RegisterDelegateWithPredecessors` handler in
    /// `crates/core/src/contract/executor/runtime/delegates.rs`) no longer
    /// calls this, because the `origin_contract` this method's H1 gate relies
    /// on is forgeable by any HTTP client — see GHSA-824h-7x5x-wfmf for the exploit chain.
    /// Kept (not deleted) so the underlying `SecretsStore::migrate_secrets`
    /// mechanism, which is otherwise sound, is easy to re-wire once
    /// `origin_contract` attestation is hardened.
    #[allow(dead_code)]
    pub(crate) fn migrate_delegate_secrets(
        &mut self,
        predecessors: &[DelegateKey],
        successor: &DelegateKey,
        origin_contract: Option<[u8; 32]>,
    ) -> super::MigrationReport {
        self.secret_store
            .migrate_secrets(predecessors, successor, origin_contract)
    }

    /// Durably record the web-app origin under which `delegate` was registered,
    /// for the H1 same-origin copy-forward gate (#4117). Called on every
    /// registration, BEFORE it is registered. Another route to the
    /// `pub(super) secret_store` from the executor module tree, like
    /// `migrate_delegate_secrets`.
    ///
    /// Propagates the store's error: a persistence failure MUST fail the whole
    /// registration (see `SecretsStore::record_delegate_registration_origin`).
    pub(crate) fn record_delegate_registration_origin(
        &self,
        delegate: &DelegateKey,
        origin: Option<[u8; 32]>,
    ) -> Result<(), super::SecretStoreError> {
        self.secret_store
            .record_delegate_registration_origin(delegate, origin)
    }

    pub fn build_with_config(
        contract_store: ContractStore,
        delegate_store: DelegateStore,
        secret_store: SecretsStore,
        host_mem: bool,
        config: RuntimeConfig,
    ) -> RuntimeResult<Self> {
        let budget = config.module_cache_budget_bytes;

        let engine = Engine::new(&config, host_mem)?;

        Ok(Self {
            engine,

            secret_store,
            delegate_store,
            contract_modules: Arc::new(Mutex::new(ModuleCache::new(budget))),

            contract_store,
            delegate_modules: Arc::new(Mutex::new(ModuleCache::new(budget))),
            delegate_contexts: super::native_api::new_delegate_context_cache(),
            created_delegates_count: super::native_api::new_delegate_counter(),
            inherited_origins: super::native_api::new_inherited_origins(),
            state_store_db: None,
            state_write_callback: None,
            state_admit_callback: None,
        })
    }

    pub fn build(
        contract_store: ContractStore,
        delegate_store: DelegateStore,
        secret_store: SecretsStore,
        host_mem: bool,
    ) -> RuntimeResult<Self> {
        Self::build_with_config(
            contract_store,
            delegate_store,
            secret_store,
            host_mem,
            RuntimeConfig::default(),
        )
    }

    /// Build a runtime that shares compiled module caches AND the backend engine
    /// with other runtimes.
    ///
    /// Used by `RuntimePool` to avoid duplicating compiled WASM modules across
    /// pool executors. Each executor gets its own Store (runtime state:
    /// memories, globals, instances), but all share the same backend engine
    /// (compiler) and module cache.
    ///
    /// # Safety requirement
    ///
    /// All runtimes sharing a module cache MUST use the same backend engine.
    /// Compiled modules store references to the compiling Engine's internal data
    /// structures. Using a Module compiled by one Engine in a Store backed by a
    /// different Engine causes SIGSEGV.
    // Each parameter is a distinct shared resource the pool wires through
    // explicitly; bundling them into a struct just to satisfy the lint
    // would obscure which executor sees which cache.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build_with_shared_module_caches(
        contract_store: ContractStore,
        delegate_store: DelegateStore,
        secret_store: SecretsStore,
        host_mem: bool,
        contract_modules: SharedModuleCache<CodeHash>,
        delegate_modules: SharedModuleCache<CodeHash>,
        delegate_contexts: super::native_api::DelegateContextCache,
        created_delegates_count: super::native_api::SharedDelegateCounter,
        inherited_origins: super::native_api::SharedInheritedOrigins,
        shared_backend: BackendEngine,
        config: &RuntimeConfig,
    ) -> RuntimeResult<Self> {
        // The pre-built `contract_modules`/`delegate_modules` caches carry the
        // byte budget (the pool sizes them in `RuntimePool::new`). `config`
        // here carries `offload_compilation` (and execution/metering knobs)
        // through to the engine — previously this hardcoded
        // `RuntimeConfig::default()`, which left `offload_compilation` dead on
        // the production pool path.
        let engine = Engine::new_with_shared_backend(config, host_mem, shared_backend)?;
        Ok(Self {
            engine,
            secret_store,
            delegate_store,
            contract_modules,
            contract_store,
            delegate_modules,
            delegate_contexts,
            created_delegates_count,
            inherited_origins,
            state_store_db: None,
            state_write_callback: None,
            state_admit_callback: None,
        })
    }

    /// Explicitly clean up a running instance from the engine.
    ///
    /// This removes the WASM `Instance` from the engine's HashMap and
    /// the MEM_ADDR entry. Should be called after the instance is no longer
    /// needed (after all WASM calls are complete).
    pub(super) fn drop_running_instance(&mut self, running: &mut RunningInstance) {
        self.engine.drop_instance(&running.handle);
        running.dropped_from_engine = true;
    }

    pub(super) fn init_buf<T>(
        &mut self,
        handle: &InstanceHandle,
        data: T,
    ) -> RuntimeResult<BufferMut<'_>>
    where
        T: AsRef<[u8]>,
    {
        let data = data.as_ref();
        // classify_result: a guest-entry epoch interrupt → Timeout class (#4864 round-5).
        let builder_ptr =
            super::classify_result(self.engine.initiate_buffer(handle, data.len() as u32))?;
        let linear_mem = self.linear_mem(handle)?;
        // SAFETY: `builder_ptr` is returned by the WASM allocator and points to a valid
        // `BufferBuilder` within the instance's linear memory described by `linear_mem`.
        unsafe {
            Ok(BufferMut::from_ptr(
                builder_ptr as *mut BufferBuilder,
                linear_mem,
            ))
        }
    }

    pub(super) fn init_buf_with_capacity(
        &mut self,
        handle: &InstanceHandle,
        capacity: usize,
    ) -> RuntimeResult<BufferMut<'_>> {
        // classify_result: a guest-entry epoch interrupt → Timeout class (#4864 round-5).
        let builder_ptr =
            super::classify_result(self.engine.initiate_buffer(handle, capacity as u32))?;
        let linear_mem = self.linear_mem(handle)?;
        // SAFETY: `builder_ptr` is returned by the WASM allocator and points to a valid
        // `BufferBuilder` within the instance's linear memory described by `linear_mem`.
        unsafe {
            Ok(BufferMut::from_ptr(
                builder_ptr as *mut BufferBuilder,
                linear_mem,
            ))
        }
    }

    /// Write data into a streaming buffer with a `[total_len: u32]` header.
    ///
    /// Allocates a buffer of at most `max_cap` bytes, writes the header and
    /// as much data as fits. If the data exceeds the buffer capacity, the
    /// remainder is stored in `CONTRACT_IO` for on-demand refill.
    pub(super) fn write_streaming_buf(
        &mut self,
        handle: &InstanceHandle,
        instance_id: i64,
        data: &[u8],
        max_cap: usize,
    ) -> RuntimeResult<*mut BufferBuilder> {
        use super::native_api::{CONTRACT_IO, PendingContractData};

        // Header: 4 bytes for total payload length (LE u32)
        let header_size = 4usize;
        debug_assert!(max_cap >= header_size, "max_cap must be >= {header_size}");
        if data.len() > u32::MAX as usize {
            return Err(super::ContractExecError::InvalidArrayLength(data.len()).into());
        }
        let buf_cap = max_cap.min(data.len().saturating_add(header_size));
        let mut buf = self.init_buf_with_capacity(handle, buf_cap)?;

        let total_len = data.len() as u32;
        buf.write(total_len.to_le_bytes())?;

        // Write as much data as fits in the remaining capacity
        let first_chunk_size = data.len().min(buf_cap - header_size);
        buf.write(&data[..first_chunk_size])?;

        let ptr = buf.ptr();

        // Store remainder for the fill callback if data didn't fit
        if first_chunk_size < data.len() {
            CONTRACT_IO.insert(
                (instance_id, ptr as i64),
                PendingContractData {
                    data: data[first_chunk_size..].to_vec(),
                    cursor: 0,
                },
            );
        }

        Ok(ptr)
    }

    /// Write data into a WASM buffer, choosing between the streaming protocol
    /// (for contracts compiled against stdlib >= 0.3.4) and the legacy one-shot
    /// protocol (for older contracts).
    pub(super) fn write_contract_buf(
        &mut self,
        running: &RunningInstance,
        data: &[u8],
        max_cap: usize,
    ) -> RuntimeResult<*mut BufferBuilder> {
        if running.supports_streaming {
            self.write_streaming_buf(&running.handle, running.id, data, max_cap)
        } else {
            let mut buf = self.init_buf(&running.handle, data)?;
            buf.write(data)?;
            Ok(buf.ptr())
        }
    }

    /// Write bincode-serialized data into a WASM buffer, choosing between
    /// streaming and legacy protocols.
    pub(super) fn write_contract_buf_serialized<T: serde::Serialize + ?Sized>(
        &mut self,
        running: &RunningInstance,
        value: &T,
        max_cap: usize,
    ) -> RuntimeResult<*mut BufferBuilder> {
        if running.supports_streaming {
            let serialized = bincode::serialize(value)?;
            self.write_streaming_buf(&running.handle, running.id, &serialized, max_cap)
        } else {
            let size = bincode::serialized_size(value)? as usize;
            let mut buf = self.init_buf_with_capacity(&running.handle, size)?;
            bincode::serialize_into(&mut buf, value)?;
            Ok(buf.ptr())
        }
    }

    pub(super) fn linear_mem(&mut self, handle: &InstanceHandle) -> RuntimeResult<WasmLinearMem> {
        let (ptr, size) = self.engine.memory_info(handle)?;
        // SAFETY: `ptr` and `size` come from the engine's live memory export for this
        // instance, so they describe a valid, allocated linear memory region.
        Ok(unsafe { WasmLinearMem::new(ptr, size as u64) })
    }

    /// Compile and instantiate a stored contract without calling any of its exports.
    ///
    /// Used by the conformance tooling so that malformed WASM, or a module missing
    /// the contract ABI, fails at the point the caller says "load this contract"
    /// rather than surfacing later as an inconclusive check result. The distinction
    /// matters because "the contract could not be loaded" and "the contract could
    /// not be judged" look identical to a caller otherwise, and the first should be
    /// a hard error while the second must never be one.
    pub(crate) fn compile_check(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
    ) -> RuntimeResult<()> {
        let _instance = self.prepare_contract_call(key, parameters, 0)?;
        Ok(())
    }

    pub(super) fn prepare_contract_call(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters,
        req_bytes: usize,
    ) -> RuntimeResult<RunningInstance> {
        self.prepare_contract_call_inner(key, parameters, req_bytes, None)
    }

    /// Like [`Self::prepare_contract_call`], but lets the caller supply the
    /// contract it already has in hand (`already_fetched`) to use on a
    /// module-cache miss, instead of re-fetching it from `contract_store`.
    ///
    /// This closes the store→fetch round-trip described in issue #2216:
    /// `verify_and_store_contract` hands a freshly-received `ContractContainer`
    /// to `contract_store.store_contract`, then immediately needed the same
    /// bytes again to compile the module on the (guaranteed, for a brand-new
    /// contract) cache miss. Passing the contract through means that path
    /// never re-reads what it just wrote.
    ///
    /// When `already_fetched` is `None` this is behaviorally identical to
    /// `prepare_contract_call` (used by every call site that doesn't already
    /// hold the contract, e.g. `update_state`/`summarize_state` on a contract
    /// that's merely being re-validated).
    pub(super) fn prepare_contract_call_with_contract(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters,
        req_bytes: usize,
        already_fetched: &ContractContainer,
    ) -> RuntimeResult<RunningInstance> {
        self.prepare_contract_call_inner(key, parameters, req_bytes, Some(already_fetched))
    }

    fn prepare_contract_call_inner(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters,
        req_bytes: usize,
        already_fetched: Option<&ContractContainer>,
    ) -> RuntimeResult<RunningInstance> {
        // Resolve the CODE hash this instance runs, which is what the compiled
        // module is keyed by (issue #5268). Compilation only ever sees the WASM
        // code — parameters are written into linear memory at call time — so N
        // instances of one contract share one compiled module.
        //
        // The hash must NOT come from `key.code_hash()`: that is an unverified
        // serde field which `ContractKey`'s `Hash`/`Eq` never consult, so a
        // caller naming an instance it is entitled to could otherwise choose
        // WHICH cached module ran for it (the same reasoning as
        // `ContractStore::fetch_contract`). Instead it comes from the node's own
        // instance index, or — for a caller holding a not-yet-indexed container
        // — from hashing the very bytes we are about to compile.
        let code_hash = match already_fetched {
            Some(contract) => wasm_code_hash(contract)?,
            None => self
                .contract_store
                .code_hash_from_id(key.id())
                .ok_or_else(|| {
                    tracing::error!(
                        contract = %key,
                        phase = "prepare_contract_call_failed",
                        "Contract not indexed in store during WASM execution"
                    );
                    RuntimeInnerError::ContractNotFound(*key)
                })?,
        };
        // Check shared cache first. The lock is held only for the duration of
        // the lookup + Module clone (an Arc bump) and is ALWAYS dropped before
        // the compile below — never held across the blocking compile.
        let cached = self
            .contract_modules
            .lock()
            .unwrap()
            .get(&code_hash)
            .cloned();
        let module = if let Some(module) = cached {
            tracing::debug!(contract = %key, %code_hash, "Module cache hit");
            module
        } else {
            tracing::info!(contract = %key, %code_hash, "Module cache miss — compiling");
            // Cache miss — obtain the code and compile with the lock released
            // so the (potentially multi-hundred-millisecond) Cranelift compile
            // never blocks other executors waiting on the shared cache. When
            // `offload_compilation` is set, `engine.compile` further offloads
            // the compile to a blocking thread so it does not pin the
            // single-threaded contract-handling loop (issue #4441).
            //
            // Prefer the caller-supplied contract (already in hand — no need
            // to round-trip through `contract_store`); fall back to fetching
            // from the store for callers that don't have it (issue #2216).
            let owned_contract;
            let contract = match already_fetched {
                Some(contract) => contract,
                None => {
                    owned_contract = self
                        .contract_store
                        .fetch_contract(key, parameters)
                        .ok_or_else(|| {
                            tracing::error!(
                                contract = %key,
                                key_code_hash = ?key.code_hash(),
                                phase = "prepare_contract_call_failed",
                                "Contract not found in store during WASM execution"
                            );
                            RuntimeInnerError::ContractNotFound(*key)
                        })?;
                    &owned_contract
                }
            };
            let code = match contract {
                ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract_v1)) => {
                    contract_v1.code().data().to_vec()
                }
                ContractContainer::Wasm(_) | _ => unimplemented!(),
            };
            let module = self.engine.compile(&code)?;
            let compiled_size = self.engine.module_compiled_size(&module);
            // Re-check cache: the lock was released before compilation, so
            // another executor may have compiled and cached this contract
            // (the per-hash coalescing mutex in the engine prevents the
            // duplicate Cranelift work, but two distinct misses can still race
            // to this insert). Prefer the already-cached clone if present.
            let mut cache = self.contract_modules.lock().unwrap();
            if let Some(existing) = cache.get(&code_hash).cloned() {
                existing
            } else {
                cache.insert(code_hash, module.clone(), compiled_size);
                module
            }
        };
        RunningInstance::new(
            &mut self.engine,
            &module,
            Key::Contract(*key.id()),
            req_bytes,
        )
    }

    /// Prepare a delegate for execution and detect its API version.
    ///
    /// Returns the running instance and the detected API version (V1 or V2).
    /// V2 is detected by inspecting whether the WASM module imports the
    /// `freenet_delegate_contracts` namespace (async host functions).
    pub(super) fn prepare_delegate_call(
        &mut self,
        params: &Parameters,
        key: &DelegateKey,
        req_bytes: usize,
    ) -> RuntimeResult<(RunningInstance, DelegateApiVersion)> {
        // Same defect and same fix as the contract cache above (#5268):
        // `prepare_delegate_call` compiles `delegate.code()` alone, but
        // `DelegateKey`'s identity covers `key = BLAKE3(code_hash ‖ params)`, so
        // keying by it compiled and retained one copy of identical machine code
        // per PARAMETER SET — the shape per-user / per-room parameterized
        // delegates hit hardest. The hash is resolved through this node's own
        // delegate index for the same trust reason: `key.code_hash()` is
        // unverified serde data, so a caller could otherwise name a delegate
        // while choosing which cached module ran for it.
        let code_hash = self
            .delegate_store
            .code_hash_from_key(key)
            .ok_or_else(|| RuntimeInnerError::DelegateNotFound(key.clone()))?;
        // Lock held only for the lookup + Module clone; always dropped before
        // the compile below (never held across the blocking compile).
        let cached = self
            .delegate_modules
            .lock()
            .unwrap()
            .get(&code_hash)
            .cloned();
        let module = if let Some(module) = cached {
            tracing::debug!(delegate = %key, %code_hash, "Module cache hit");
            module
        } else {
            tracing::info!(delegate = %key, %code_hash, "Module cache miss — compiling");
            let delegate = self
                .delegate_store
                .fetch_delegate(key, params)
                .ok_or_else(|| RuntimeInnerError::DelegateNotFound(key.clone()))?;
            let code = delegate.code().as_ref().to_vec();
            let module = self.engine.compile(&code)?;
            let compiled_size = self.engine.module_compiled_size(&module);
            // Re-check cache: the lock was released before compilation, so
            // another executor may have compiled and cached this delegate.
            let mut cache = self.delegate_modules.lock().unwrap();
            if let Some(existing) = cache.get(&code_hash).cloned() {
                existing
            } else {
                cache.insert(code_hash, module.clone(), compiled_size);
                module
            }
        };

        let api_version = if self.engine.module_has_async_imports(&module) {
            DelegateApiVersion::V2
        } else {
            DelegateApiVersion::V1
        };

        let running = RunningInstance::new(
            &mut self.engine,
            &module,
            Key::Delegate(key.clone()),
            req_bytes,
        )?;
        Ok((running, api_version))
    }
}

impl super::contract::ContractStoreBridge for Runtime {
    fn code_hash_from_id(&self, id: &ContractInstanceId) -> Option<CodeHash> {
        self.contract_store.code_hash_from_id(id)
    }

    fn fetch_contract_code(
        &self,
        key: &ContractKey,
        params: &Parameters<'_>,
    ) -> Option<ContractContainer> {
        self.contract_store.fetch_contract(key, params)
    }

    fn code_blob_stored(&self, code_hash: &CodeHash) -> bool {
        self.contract_store.code_blob_stored(code_hash)
    }

    fn store_contract(&mut self, contract: ContractContainer) -> Result<(), anyhow::Error> {
        self.contract_store.store_contract(contract)?;
        Ok(())
    }

    fn remove_contract(&mut self, key: &ContractKey) -> Result<(), anyhow::Error> {
        self.contract_store.remove_contract(key)?;
        Ok(())
    }
}

impl super::contract::ContractRuntimeBridge for Runtime {}

#[cfg(test)]
mod wasmtime_disk_cache_sizing_tests {
    use super::{
        MAX_WASMTIME_CACHE_SIZE_BYTES, MIN_WASMTIME_CACHE_SIZE_BYTES,
        WASMTIME_CACHE_FALLBACK_TOTAL_RAM_BYTES, wasmtime_cache_size_for_ram,
    };
    use crate::ring::hosting_budget_for_ram;

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;

    /// The flat value the on-disk compile-cache soft limit used to carry
    /// regardless of machine size. Stated here (not imported) so the tests below
    /// pin the OLD behavior they replace without re-introducing a production
    /// constant.
    const LEGACY_FLAT_SOFT_LIMIT_BYTES: u64 = 512 * MIB;

    /// p90 on-disk artifact size measured on the production gateway (nova,
    /// `~freenet/.local/share/freenet/wasmtime-cache`, 2026-07-28): 418
    /// artifacts, 198.8 MiB total, mean 487 KiB, p50 397 KiB, p90 811 KiB, max
    /// 1.70 MiB. These are zstd-compressed on-disk entries, NOT the in-memory
    /// `Module::serialize()` size.
    const MEASURED_P90_ARTIFACT_BYTES: u64 = 811 * 1024;

    /// Entry count the floor must still buy at that measured p90. Chosen from
    /// the measurement (128 MiB / 811 KiB ≈ 161) with room to spare, so this
    /// trips if [`MIN_WASMTIME_CACHE_SIZE_BYTES`] is lowered.
    const MIN_ENTRIES_THE_FLOOR_MUST_HOLD: u64 = 150;

    /// The three machine shapes this change was sized against, plus the strict
    /// monotonicity a constant-returning implementation cannot satisfy.
    #[test]
    fn compile_cache_scales_with_the_memory_the_node_may_use() {
        // The measured peer: a laptop under a 2 GiB cgroup, whose contract-state
        // budget is 256 MiB. It used to get a 512 MiB compile cache — larger
        // than the data it accelerates — and now gets 256 MiB.
        assert_eq!(wasmtime_cache_size_for_ram(2 * GIB), 256 * MIB);
        assert!(
            wasmtime_cache_size_for_ram(2 * GIB) < LEGACY_FLAT_SOFT_LIMIT_BYTES,
            "the containerized peer must get LESS than the old flat limit"
        );

        // A 15 GiB VM and a 125 GiB server are both far past the point where the
        // ceiling binds, so they keep exactly the historical 512 MiB: this is a
        // "small nodes get less" change, never a cache increase.
        assert_eq!(wasmtime_cache_size_for_ram(15 * GIB), 512 * MIB);
        assert_eq!(wasmtime_cache_size_for_ram(125 * GIB), 512 * MIB);

        // Strictly node-relative between the clamps: a bigger host gets a bigger
        // cache. A fixed constant (the defect) fails every line here.
        assert_eq!(wasmtime_cache_size_for_ram(3 * GIB), 384 * MIB);
        assert!(wasmtime_cache_size_for_ram(2 * GIB) < wasmtime_cache_size_for_ram(3 * GIB));
        assert!(wasmtime_cache_size_for_ram(3 * GIB) < wasmtime_cache_size_for_ram(4 * GIB));
    }

    /// Floor boundary. Every expectation is a CONCRETE byte value rather than a
    /// comparison against `MIN_WASMTIME_CACHE_SIZE_BYTES`: an assertion written
    /// against the constant is self-referential and would still pass if the
    /// floor were mutated to 0 (0 == 0), which is exactly the shape that makes a
    /// floor test look like coverage it does not have.
    #[test]
    fn compile_cache_floor_binds_on_tiny_hosts() {
        // Degenerate inputs must not produce a zero-size (recompile-everything)
        // cache. Concrete value, so floor→0 fails here.
        assert_eq!(wasmtime_cache_size_for_ram(0), 128 * MIB);
        assert_eq!(wasmtime_cache_size_for_ram(1), 128 * MIB);

        // A 512 MiB VPS: the raw divisor gives 64 MiB, the floor lifts it.
        assert_eq!(wasmtime_cache_size_for_ram(512 * MIB), 128 * MIB);

        // Exactly at the binding point: 1 GiB / 8 == 128 MiB == the floor.
        assert_eq!(wasmtime_cache_size_for_ram(GIB), 128 * MIB);
        // One divisor-step above it the derived value takes over, so the floor
        // is a floor and not a second constant.
        assert_eq!(wasmtime_cache_size_for_ram(GIB + 8), 128 * MIB + 1);
    }

    /// Guard on the floor CONSTANT, expressed in measured units: at the real
    /// p90 on-disk artifact size the floor must still buy a useful number of
    /// entries. This does not validate the measurement (constants cannot); its
    /// job is to trip if [`MIN_WASMTIME_CACHE_SIZE_BYTES`] is lowered to a value
    /// that stops keeping a working set warm.
    #[test]
    fn floor_holds_a_useful_entry_count_at_the_measured_artifact_size() {
        let entries_at_p90 = MIN_WASMTIME_CACHE_SIZE_BYTES / MEASURED_P90_ARTIFACT_BYTES;
        assert!(
            entries_at_p90 >= MIN_ENTRIES_THE_FLOOR_MUST_HOLD,
            "the {MIN_WASMTIME_CACHE_SIZE_BYTES}-byte floor holds only {entries_at_p90} \
             artifacts at the measured p90 of {MEASURED_P90_ARTIFACT_BYTES} bytes; it must \
             hold at least {MIN_ENTRIES_THE_FLOOR_MUST_HOLD}. Lowering the floor buys disk \
             and pays for it in Cranelift recompiles."
        );
    }

    /// Ceiling boundary: large hosts stop at the historical flat value and the
    /// arithmetic cannot overflow on an absurd input. Concrete values for the
    /// same self-reference reason as the floor test.
    #[test]
    fn compile_cache_ceiling_binds_on_large_hosts() {
        // One divisor-step below the binding point the derived value still wins.
        assert_eq!(wasmtime_cache_size_for_ram(4 * GIB - 8), 512 * MIB - 1);
        // Exactly at the binding point: 4 GiB / 8 == 512 MiB == the ceiling.
        assert_eq!(wasmtime_cache_size_for_ram(4 * GIB), 512 * MIB);
        assert_eq!(wasmtime_cache_size_for_ram(8 * GIB), 512 * MIB);
        // u64::MAX must clamp, not wrap or panic.
        assert_eq!(wasmtime_cache_size_for_ram(u64::MAX), 512 * MIB);
    }

    /// The compile-cache DEFAULT never exceeds the hosted-state DEFAULT for the
    /// same host, at any host size. Both derive from the same "memory the node
    /// may use" signal with the same divisor and floor, and the compile cache
    /// has the strictly lower ceiling.
    ///
    /// SCOPE — read before restating this anywhere: it relates two DEFAULT
    /// functions, NOT two live budgets. The hosted-state budget is
    /// operator-overridable (`--max-hosting-storage` / `MAX_HOSTING_STORAGE`)
    /// and the compile-cache limit is not overridable at all, so a node with an
    /// overridden state budget can absolutely carry a larger compile cache than
    /// state budget. The final assertion below demonstrates that counterexample
    /// on purpose, so this test cannot be misread as a system-level invariant.
    #[test]
    fn compile_cache_default_never_exceeds_hosting_default() {
        for total_ram in [
            0,
            1,
            128 * MIB,
            512 * MIB,
            GIB,
            2 * GIB, // the measured cgroup-limited peer
            3 * GIB,
            4 * GIB,
            8 * GIB,
            15 * GIB, // VM
            32 * GIB,
            125 * GIB, // server
            u64::MAX,
        ] {
            let compile_cache = wasmtime_cache_size_for_ram(total_ram);
            let state_budget = hosting_budget_for_ram(total_ram);
            assert!(
                compile_cache <= state_budget,
                "at total_ram={total_ram} the DEFAULT on-disk compile cache \
                 ({compile_cache}) must not exceed the DEFAULT contract-state budget \
                 ({state_budget})"
            );
        }

        // The exact shape from the defect report: a 2 GiB-cgroup peer whose
        // entire contract-state budget is 256 MiB used to permit a 512 MiB
        // compile cache (and was measured holding ~306 MB).
        assert_eq!(hosting_budget_for_ram(2 * GIB), 256 * MIB);
        assert_eq!(wasmtime_cache_size_for_ram(2 * GIB), 256 * MIB);
        assert!(LEGACY_FLAT_SOFT_LIMIT_BYTES > hosting_budget_for_ram(2 * GIB));

        // COUNTEREXAMPLE (documenting the scope limit): an operator running
        // `--max-hosting-storage 64MiB` on a 4 GiB box gets a 64 MiB state
        // budget beside a 512 MiB compile cache. The ordering above is a
        // property of the two defaults only.
        let operator_overridden_state_budget = 64 * MIB;
        assert!(
            wasmtime_cache_size_for_ram(4 * GIB) > operator_overridden_state_budget,
            "an operator-overridden state budget CAN be smaller than the compile \
             cache — the ordering holds between defaults, not between live budgets"
        );
    }

    /// Host-independent pin: the live reader (`default_wasmtime_cache_size_bytes_for_dir`,
    /// #5014) must derive its value from the shared RAM signal AND the disk
    /// availability signal, delegating to the pure combiner rather than
    /// carrying its own arithmetic or re-hardcoding a flat constant. This is a
    /// source-scrape pin (not a live-value comparison) because on a host above
    /// the ceiling-binding point on BOTH axes a reader that ignores its inputs
    /// entirely would still coincidentally return the ceiling — see
    /// `wasmtime_disk_cache_disk_sizing_tests` for the live-value coverage that
    /// exercises the RAM/disk interaction itself.
    #[test]
    fn default_soft_limit_reader_derives_from_ram_and_disk_signals() {
        let src = include_str!("runtime.rs");
        let body = src
            .split("pub(crate) fn default_wasmtime_cache_size_bytes_for_dir(")
            .nth(1)
            .expect("default_wasmtime_cache_size_bytes_for_dir must exist")
            .split("\n}\n")
            .next()
            .expect("end of default_wasmtime_cache_size_bytes_for_dir");
        assert!(
            body.contains("read_total_ram_bytes()"),
            "the reader must consult the shared read_total_ram_bytes() signal, not \
             a second notion of machine size"
        );
        assert!(
            body.contains("disk_available_bytes("),
            "the reader must consult a real disk-availability signal — a RAM-only \
             reader is exactly the #5014 defect"
        );
        assert!(
            body.contains("combine_wasmtime_cache_size("),
            "the reader must delegate to the pure combiner so the RAM/disk \
             interaction has exactly one implementation"
        );
        assert!(
            body.contains("bound_by_configured_disk_budget("),
            "the reader must ALSO bound the physical-disk term by the \
             operator's configured hosting-disk budget — a physical-only \
             bound leaves an operator-shrunk --max-hosting-disk wedged"
        );
        assert!(
            body.contains("reconcile_existing_cache_dir("),
            "the reader must reconcile an already-oversized cache directory, not \
             just narrow the limit for future growth"
        );
    }

    /// The OS-query fallback is itself a legal, conservative value: an
    /// unknown-capability host must land on the floor, not the ceiling.
    #[test]
    fn fallback_ram_estimate_resolves_to_the_floor() {
        assert_eq!(
            wasmtime_cache_size_for_ram(WASMTIME_CACHE_FALLBACK_TOTAL_RAM_BYTES),
            128 * MIB,
            "a host whose RAM we cannot read must get the smallest sane cache"
        );
    }

    /// The clamps must stay ordered and the ceiling must stay at the historical
    /// flat value, so a future edit cannot silently turn this into a cache
    /// *increase* for hosts that are unaffected today.
    #[test]
    fn clamp_bounds_are_ordered_and_ceiling_is_the_historical_default() {
        // Compile-time tripwire: the clamp bounds are consts, so checking their
        // ordering at compile time catches a regression immediately rather than
        // only when this test happens to run.
        const _: () = assert!(MIN_WASMTIME_CACHE_SIZE_BYTES < MAX_WASMTIME_CACHE_SIZE_BYTES);
        assert_eq!(MAX_WASMTIME_CACHE_SIZE_BYTES, LEGACY_FLAT_SOFT_LIMIT_BYTES);
    }
}

/// #5014: the disk-side term, the RAM/disk composition, and the startup
/// reconciliation that gives an already-oversized cache immediate relief.
#[cfg(test)]
mod wasmtime_disk_cache_disk_sizing_tests {
    use super::{
        MAX_WASMTIME_CACHE_SIZE_BYTES, MIN_WASMTIME_CACHE_SIZE_BYTES,
        WASMTIME_CACHE_FALLBACK_TOTAL_RAM_BYTES, bound_by_configured_disk_budget,
        combine_wasmtime_cache_size, default_wasmtime_cache_size_bytes_for_dir,
        reconcile_existing_cache_dir, stabilize_available_disk_bytes, wasmtime_cache_size_for_disk,
        wasmtime_cache_size_for_ram,
    };
    use std::io::Write;

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;

    /// Floor/ceiling boundary for the disk-side term, mirroring
    /// `compile_cache_floor_binds_on_tiny_hosts` /
    /// `compile_cache_ceiling_binds_on_large_hosts` for the RAM-side term.
    /// Concrete byte values, not comparisons against the constants — see
    /// those tests' doc comment for why a self-referential assertion would
    /// pass even if the floor were mutated to 0.
    #[test]
    fn disk_term_floor_and_ceiling_bind() {
        // The disk-side floor is 32 MiB — deliberately LOWER than the 128 MiB
        // RAM-side/aggregate-budget floor, see
        // `MIN_WASMTIME_CACHE_SIZE_BYTES_FOR_DISK`'s doc (#5328 review).
        assert_eq!(wasmtime_cache_size_for_disk(0), 32 * MIB);
        assert_eq!(wasmtime_cache_size_for_disk(1), 32 * MIB);
        // Exactly at the binding point: 256 MiB / 8 == 32 MiB == the floor.
        assert_eq!(wasmtime_cache_size_for_disk(256 * MIB), 32 * MIB);
        // One divisor-step above it the derived value takes over.
        assert_eq!(wasmtime_cache_size_for_disk(256 * MIB + 8), 32 * MIB + 1);
        // Exactly at the binding point: 4 GiB / 8 == 512 MiB == the ceiling.
        assert_eq!(wasmtime_cache_size_for_disk(4 * GIB), 512 * MIB);
        assert_eq!(wasmtime_cache_size_for_disk(8 * GIB), 512 * MIB);
        // u64::MAX must clamp, not wrap or panic.
        assert_eq!(wasmtime_cache_size_for_disk(u64::MAX), 512 * MIB);
    }

    /// The exact shape from #5014's worked example: a 16 GiB VM (RAM ample —
    /// the RAM term resolves to the historical 512 MiB ceiling) with only
    /// 400 MiB free on the data-dir mount. Before this fix, ONLY the RAM term
    /// existed, so this host got a 512 MiB compile cache while its whole disk
    /// budget (`clamp(0.5 * (used + available), ...)`) sat far below that —
    /// wedging `admit_state_write`/`admit_wasm_write` shut. The disk term
    /// must now pull the combined result down from the RAM ceiling.
    #[test]
    fn ram_rich_disk_tight_host_is_bounded_by_the_disk_term() {
        let ram_only = wasmtime_cache_size_for_ram(16 * GIB);
        assert_eq!(
            ram_only,
            512 * MIB,
            "16 GiB RAM must hit the RAM-side ceiling"
        );

        let available_disk = 400 * MIB;
        let combined = combine_wasmtime_cache_size(16 * GIB, Some(available_disk));
        assert!(
            combined < ram_only,
            "a disk-tight host (400 MiB free) must get LESS than the RAM-only \
             figure ({ram_only}); got {combined}"
        );
        // 400 MiB / 8 == 50 MiB — above the disk-side floor (32 MiB), so the
        // raw division binds here, not a floor (#5328 review: an earlier
        // version of this test asserted the value landed exactly on the
        // shared 128 MiB floor, which mutation-tested green even when the
        // disk divisor was changed from 8 to 64 — it was pinning the floor,
        // not the disk term. This value is a genuine division result.)
        assert_eq!(combined, 50 * MIB);
    }

    /// The composition is `min(ram_term, disk_term)` — whichever signal is
    /// tighter wins, in both directions.
    #[test]
    fn combine_takes_the_tighter_of_the_two_terms() {
        // RAM-poor, disk-rich: the RAM term binds (unchanged from before #5014).
        assert_eq!(
            combine_wasmtime_cache_size(2 * GIB, Some(100 * GIB)),
            wasmtime_cache_size_for_ram(2 * GIB),
        );
        // RAM-rich, disk-poor: the disk term binds (the #5014 fix).
        assert_eq!(
            combine_wasmtime_cache_size(100 * GIB, Some(2 * GIB)),
            wasmtime_cache_size_for_disk(2 * GIB),
        );
        // Both ample: both clamp to the shared ceiling, so it's a no-op either way.
        assert_eq!(
            combine_wasmtime_cache_size(100 * GIB, Some(100 * GIB)),
            MAX_WASMTIME_CACHE_SIZE_BYTES,
        );
    }

    /// An unreadable disk signal (statvfs failure / unsupported platform)
    /// must NOT invent a possibly-wrong tight cap — it falls back to the
    /// RAM-only figure, i.e. today's shipped behavior, unchanged.
    #[test]
    fn unreadable_disk_signal_falls_back_to_ram_only() {
        assert_eq!(
            combine_wasmtime_cache_size(16 * GIB, None),
            wasmtime_cache_size_for_ram(16 * GIB),
        );
    }

    /// A directory already over the newly-computed limit (the "upgrading an
    /// already-wedged gateway" case) is cleared immediately rather than left
    /// for wasmtime's own ~1h prune cycle.
    #[test]
    fn reconcile_clears_a_directory_already_over_the_new_limit() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("wasmtime-cache");
        std::fs::create_dir_all(&cache_dir).unwrap();
        let mut f = std::fs::File::create(cache_dir.join("big.bin")).unwrap();
        f.write_all(&vec![0u8; 200 * 1024 * 1024]).unwrap(); // 200 MiB

        reconcile_existing_cache_dir(&cache_dir, 200 * MIB, 128 * MIB);

        assert!(
            !cache_dir.exists(),
            "an over-limit cache directory must be cleared, not left for the \
             ~1h wasmtime prune cycle to catch up"
        );
    }

    /// A directory already AT OR UNDER the limit must be left alone — this is
    /// a startup optimization for the over-limit case, not an unconditional
    /// wipe of the compile cache on every boot.
    #[test]
    fn reconcile_leaves_a_directory_under_the_limit_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("wasmtime-cache");
        std::fs::create_dir_all(&cache_dir).unwrap();
        let mut f = std::fs::File::create(cache_dir.join("small.bin")).unwrap();
        f.write_all(&vec![0u8; 1024]).unwrap(); // 1 KiB

        reconcile_existing_cache_dir(&cache_dir, 1024, 128 * MIB);

        assert!(
            cache_dir.join("small.bin").exists(),
            "a directory already under the limit must not be touched"
        );
    }

    /// A missing directory (fresh node, nothing written yet) must be a no-op,
    /// not a panic or an error — `du_walk`'s own contract for a missing dir is
    /// "contributes 0", so 0 is never `>` any real limit.
    #[test]
    fn reconcile_is_a_no_op_on_a_missing_directory() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist-yet");
        reconcile_existing_cache_dir(&missing, 0, 128 * MIB); // must not panic
        assert!(!missing.exists());
    }

    /// Smoke test for the impure entry point end-to-end against a real
    /// directory: whatever this host's actual RAM/disk resolve to, the result
    /// must stay within the shared clamp bounds, and it must not panic when
    /// run against a directory that doesn't exist yet (the fresh-node case).
    #[test]
    fn default_for_dir_stays_within_bounds_when_the_directory_does_not_exist_yet() {
        // Defensive-only: in production `config.rs` always `create_dir_all`s
        // this directory before `Executor::from_config_with_shared_modules`
        // ever runs, so this exact input never reaches this function on a real
        // node. Kept as a "must not panic, must fall back sanely" guard, not
        // as a stand-in for the real disk-derived path — see the sibling test
        // below for that.
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("wasmtime-cache");
        let result = default_wasmtime_cache_size_bytes_for_dir(
            &missing,
            crate::ring::DEFAULT_HOSTING_DISK_PCT,
            crate::ring::DEFAULT_MAX_HOSTING_DISK_BYTES,
        );
        assert!(
            (MIN_WASMTIME_CACHE_SIZE_BYTES..=MAX_WASMTIME_CACHE_SIZE_BYTES).contains(&result),
            "result {result} must stay within [{MIN_WASMTIME_CACHE_SIZE_BYTES}, \
             {MAX_WASMTIME_CACHE_SIZE_BYTES}] regardless of this host's real RAM/disk"
        );
    }

    /// #5328 review: the fresh-directory test above never actually created the
    /// directory, so `statvfs` returned ENOENT and it silently exercised the
    /// SAME `None`-fallback path as `unreadable_disk_signal_falls_back_to_ram_only`
    /// — never the real `Some(...)` disk-reading path a production node
    /// actually takes (the cache dir always exists by the time this runs; see
    /// the sibling test's comment). This test creates the directory first, so
    /// `disk_available_bytes` succeeds, and cross-checks the live entry
    /// point's result against the SAME real signals read independently
    /// (`super::read_total_ram_bytes()`, `crate::ring::disk_available_bytes`)
    /// and fed through the pure combiner — not a hardcoded expectation, since
    /// this host's real RAM/disk are unknown to the test. A maximally
    /// PERMISSIVE configured budget (pct=1.0, max=u64::MAX) is passed so the
    /// configured-budget bound (#5328 review) cannot additionally constrain
    /// the result — that mechanism gets its own dedicated test below.
    #[test]
    fn default_for_dir_uses_the_real_disk_reading_on_an_existing_directory() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("wasmtime-cache");
        std::fs::create_dir_all(&cache_dir).unwrap();

        let total_ram = crate::wasm_runtime::read_total_ram_bytes()
            .map(|v| v as u64)
            .unwrap_or(WASMTIME_CACHE_FALLBACK_TOTAL_RAM_BYTES);
        let available_disk_bytes = crate::ring::disk_available_bytes(&cache_dir);
        assert!(
            available_disk_bytes.is_some(),
            "statvfs on a directory that genuinely exists must succeed on this \
             platform — if this fails, the test tempdir setup is wrong, not the \
             production code"
        );
        let expected = combine_wasmtime_cache_size(total_ram, available_disk_bytes);

        assert_eq!(
            default_wasmtime_cache_size_bytes_for_dir(&cache_dir, 1.0, u64::MAX),
            expected,
            "the live entry point must match the pure combiner fed the SAME \
             real RAM/disk signals — this pins that it actually reads a live \
             Some(...) disk signal, not silently falling back to RAM-only"
        );
    }

    /// #5328 review (rev-skeptical-2 finding): an operator who shrinks
    /// `--max-hosting-disk` below physical disk capacity must ALSO be
    /// protected — bounding only by raw physical availability leaves that
    /// operator permanently wedged against their OWN configured budget. A
    /// tiny `max_hosting_disk` must pull the result down from what physical
    /// disk alone would allow, and the result must still respect the
    /// documented headroom relationship (a quarter of what
    /// `disk_budget_for_clamped` would compute for the SAME inputs).
    #[test]
    fn default_for_dir_is_bounded_by_a_tiny_configured_disk_budget() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("wasmtime-cache");
        std::fs::create_dir_all(&cache_dir).unwrap();

        // Permissive physical/RAM signals (pct=1.0 isn't physical/RAM — this
        // is the operator's CONFIGURED knob under test): a tiny
        // max_hosting_disk must bind regardless of how much physical disk or
        // RAM this test host actually has.
        let tiny_max_hosting_disk = 40 * MIB;
        let result = default_wasmtime_cache_size_bytes_for_dir(
            &cache_dir,
            crate::ring::DEFAULT_HOSTING_DISK_PCT,
            tiny_max_hosting_disk,
        );

        assert!(
            result <= tiny_max_hosting_disk,
            "a compile cache larger than the operator's OWN configured \
             --max-hosting-disk ({tiny_max_hosting_disk}) defeats the whole \
             point of the setting; got {result}"
        );
    }

    /// #5328 review finding 1 (rev-domain, verified by hand): giving the
    /// disk-side term the SAME floor as the aggregate hosting-disk budget's
    /// own floor left EXACTLY ZERO headroom for real contract state on any
    /// host with <= 256 MiB reachable disk — the compile cache alone would
    /// consume the entire disk-budget floor, so #5014's wedge would persist
    /// (narrower, but not closed) for small-disk hosts. This sweeps a wide
    /// range of reachable-disk sizes and asserts the aggregate disk budget
    /// (computed via the SAME `disk_budget_for_clamped` the real
    /// eviction/admission path uses) always leaves STRICTLY positive headroom
    /// over the compile-cache disk term — mirroring
    /// `compile_cache_default_never_exceeds_hosting_default`'s sweep shape
    /// for the RAM axis. `reachable_disk` models `used + available` at the
    /// moment the disk term is computed (worst case: the compile cache is the
    /// ONLY consumer, i.e. right after `reconcile_existing_cache_dir` clears
    /// a stale cache — the scenario most likely to wedge).
    #[test]
    fn disk_term_always_leaves_positive_headroom_against_the_aggregate_disk_budget() {
        for reachable_disk in [
            0,
            1,
            MIB,
            32 * MIB,
            64 * MIB,
            128 * MIB,
            200 * MIB,
            255 * MIB,
            256 * MIB, // the exact breakeven point both floors share
            257 * MIB,
            300 * MIB,
            400 * MIB, // the issue's worked example
            912 * MIB, // the issue's worked example, post-reconcile
            GIB,
            2 * GIB,
            4 * GIB,
            8 * GIB,
            32 * GIB,
            100 * GIB,
            u64::MAX,
        ] {
            let disk_term = wasmtime_cache_size_for_disk(reachable_disk);
            let available = reachable_disk.saturating_sub(disk_term);
            let disk_budget = crate::ring::disk_budget_for_clamped(
                disk_term,
                available,
                crate::ring::DEFAULT_HOSTING_DISK_PCT,
                crate::ring::MIN_DEFAULT_HOSTING_BUDGET_BYTES,
                crate::ring::DEFAULT_MAX_HOSTING_DISK_BYTES,
            );
            assert!(
                disk_budget > disk_term,
                "at reachable_disk={reachable_disk} the compile cache's disk \
                 term ({disk_term}) must leave POSITIVE headroom under the \
                 aggregate disk budget ({disk_budget}) for real contract \
                 state — zero or negative headroom means the compile cache \
                 alone wedges admission"
            );
        }
    }

    /// #5328 review (rev-skeptical-2 finding): `bound_by_configured_disk_budget`
    /// must actually bind when the operator's configured budget is the
    /// tighter constraint, must NOT bind when it's generous (physical term
    /// wins), and must fall back to the physical term when the disk signal
    /// is unreadable (no `used + available` basis to project a budget from).
    #[test]
    fn bound_by_configured_disk_budget_binds_only_when_tighter() {
        // Ample physical term (RAM-ceiling-bound), tiny configured budget:
        // the configured bound must win.
        let physical_term = 512 * MIB;
        let tight = bound_by_configured_disk_budget(
            physical_term,
            0,              // current_cache_bytes
            Some(40 * MIB), // raw_available_disk_bytes
            crate::ring::DEFAULT_HOSTING_DISK_PCT,
            crate::ring::MIN_DEFAULT_HOSTING_BUDGET_BYTES, // tiny max_hosting_disk
        );
        assert!(
            tight < physical_term,
            "a tiny configured max_hosting_disk must pull the result below \
             the physical term; got {tight}"
        );

        // Generous configured budget: the physical term must win unchanged.
        let generous = bound_by_configured_disk_budget(
            physical_term,
            0,
            Some(100 * GIB),
            crate::ring::DEFAULT_HOSTING_DISK_PCT,
            crate::ring::DEFAULT_MAX_HOSTING_DISK_BYTES,
        );
        assert_eq!(
            generous, physical_term,
            "a generous configured budget must not tighten the physical term"
        );

        // Unreadable disk signal: no used+available basis to project a
        // budget from, so the physical term passes through unchanged.
        let unreadable = bound_by_configured_disk_budget(
            physical_term,
            0,
            None,
            crate::ring::DEFAULT_HOSTING_DISK_PCT,
            crate::ring::MIN_DEFAULT_HOSTING_BUDGET_BYTES,
        );
        assert_eq!(
            unreadable, physical_term,
            "an unreadable disk signal must fall back to the physical term, \
             not invent a budget projection from nothing"
        );
    }

    /// #5328 review: the configured-budget bound must ALSO leave positive
    /// headroom against the real aggregate budget, across a sweep of
    /// operator-configured `max_hosting_disk` values (not just the default) —
    /// extending `disk_term_always_leaves_positive_headroom_against_the_aggregate_disk_budget`
    /// to the axis that test doesn't cover.
    #[test]
    fn configured_budget_bound_always_leaves_positive_headroom() {
        for reachable_disk in [0, MIB, 128 * MIB, 256 * MIB, GIB, 100 * GIB] {
            for max_hosting_disk in [
                crate::ring::MIN_DEFAULT_HOSTING_BUDGET_BYTES, // operator floors it
                16 * GIB,
                crate::ring::DEFAULT_MAX_HOSTING_DISK_BYTES,
            ] {
                let physical_term = wasmtime_cache_size_for_disk(reachable_disk);
                let available = reachable_disk.saturating_sub(physical_term);
                let bound = bound_by_configured_disk_budget(
                    physical_term,
                    physical_term, // current_cache_bytes: conservative, matches `used` below
                    Some(available),
                    crate::ring::DEFAULT_HOSTING_DISK_PCT,
                    max_hosting_disk,
                );
                let real_budget = crate::ring::disk_budget_for_clamped(
                    bound,
                    available,
                    crate::ring::DEFAULT_HOSTING_DISK_PCT,
                    crate::ring::MIN_DEFAULT_HOSTING_BUDGET_BYTES,
                    max_hosting_disk,
                );
                assert!(
                    real_budget > bound,
                    "at reachable_disk={reachable_disk}, \
                     max_hosting_disk={max_hosting_disk}: the configured-budget-\
                     bound compile cache ({bound}) must leave POSITIVE headroom \
                     under the real aggregate budget ({real_budget})"
                );
            }
        }
    }

    /// #5328 review finding 2 (rev-domain, verified by hand): folding the
    /// directory's own current size back into the raw free-space reading
    /// must recover "total reachable capacity", independent of how big the
    /// directory currently is. Deliberately host-independent (small,
    /// hand-chosen numbers) — see [`stabilize_available_disk_bytes`]'s doc
    /// for why a real end-to-end test through actual `statvfs` cannot
    /// reliably distinguish fixed from buggy on a host with generous free
    /// disk.
    #[test]
    fn stabilize_available_disk_bytes_recovers_total_reachable_capacity() {
        // 400 MiB total; the cache currently occupies 50 MiB of it, so a raw
        // statvfs read sees only 350 MiB free. Folding the cache's own 50 MiB
        // back in must recover the full 400 MiB.
        assert_eq!(
            stabilize_available_disk_bytes(Some(350 * MIB), 50 * MIB),
            Some(400 * MIB)
        );
        // An empty directory contributes nothing to fold back — a no-op.
        assert_eq!(
            stabilize_available_disk_bytes(Some(400 * MIB), 0),
            Some(400 * MIB)
        );
        // An unreadable raw signal must stay unreadable — folding a KNOWN
        // quantity into an UNKNOWN one must not manufacture a trusted result.
        assert_eq!(stabilize_available_disk_bytes(None, 50 * MIB), None);
        // Overflow-safe: saturating, never panics or wraps.
        assert_eq!(
            stabilize_available_disk_bytes(Some(u64::MAX), 50 * MIB),
            Some(u64::MAX)
        );
    }

    /// #5328 review finding 2 (rev-domain, verified by hand): a naive
    /// `statvfs`-only availability reading makes the computed limit a
    /// function of the cache's OWN current size (bigger cache -> less
    /// "available" -> smaller next-boot limit -> wipe -> more "available" ->
    /// bigger limit -> cache regrows -> repeat), which converges to wiping
    /// the compile cache on roughly every OTHER restart for an actively-used
    /// node — defeating the entire point of a persistent on-disk cache. This
    /// simulates two successive "boots" against a FIXED total reachable disk
    /// (deterministic, no real filesystem involved): boot 1 computes a limit
    /// against an empty cache; the cache then regrows to fill exactly that
    /// limit (the worst case — a busy node whose cache regrew to fill its
    /// budget between restarts, so the raw free-space reading on boot 2 is
    /// `total - limit1`); boot 2 must compute the SAME limit via
    /// [`stabilize_available_disk_bytes`]'s fold-back — and the final
    /// assertion demonstrates, on the SAME numbers, that WITHOUT the
    /// fold-back the limit would have shrunk (the bug this fixes).
    #[test]
    fn folding_the_caches_own_size_back_in_makes_the_limit_stable_across_simulated_restarts() {
        let total_ram = 100 * GIB; // ample — only the disk term can bind here
        let total_reachable_disk = 2 * GIB; // fixed total capacity, both boots

        // Boot 1: cache is empty, so raw free space IS the total.
        let limit1 = combine_wasmtime_cache_size(
            total_ram,
            stabilize_available_disk_bytes(Some(total_reachable_disk), 0),
        );

        // Between boots: cache regrows to fill exactly limit1. Raw free space
        // on boot 2 is reduced by exactly what the cache now occupies.
        let raw_available_boot2 = total_reachable_disk - limit1;
        let limit2 = combine_wasmtime_cache_size(
            total_ram,
            stabilize_available_disk_bytes(Some(raw_available_boot2), limit1),
        );

        assert_eq!(
            limit1, limit2,
            "the computed limit must be STABLE across restarts when nothing \
             other than the cache's own regrowth changed on disk"
        );

        // Sanity: on these SAME numbers, the fold-back is load-bearing — a
        // raw (unstabilized) reading on boot 2 computes a SMALLER limit,
        // which is exactly what would trigger reconcile's wipe.
        let unstabilized_limit2 = combine_wasmtime_cache_size(total_ram, Some(raw_available_boot2));
        assert!(
            unstabilized_limit2 < limit1,
            "sanity check failed: this scenario no longer demonstrates the \
             bug the fold-back fixes, so it's not exercising anything — \
             unstabilized_limit2={unstabilized_limit2}, limit1={limit1}"
        );
    }
}
