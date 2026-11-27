//! Bounding the CPU time and memory of compiling contract code.
//!
//! A module is first scanned in the node (see `compile_limits`). With this
//! module enabled, a hard-capped worker process then judges it: the node's own
//! binary, re-run with a hidden argument, runs the same scan and the same
//! `Module::new` on the same thread pool under kernel limits (CPU time, private
//! memory), a wall-clock limit and a parent-death signal. The worker returns only
//! a verdict. The node never loads anything from it: after an "accepted" verdict
//! it compiles the bytes itself, as it always did, so a worker that returns
//! a wrong verdict can only make the node compile code the worker should have
//! refused, which is what happens without a worker.
//!
//! The scan is a coarse pre-filter, not a cost bound: it is deliberately loose
//! enough never to refuse a real artifact. What bounds the cost of a module that
//! passes it is the worker's CPU, memory and wall limits.
//!
//! Modes (config `compile-isolation`): `off` (the default; nothing here runs),
//! `dry-run` (a worker judges the module on a thread of its own, beside the
//! node's compile, and the verdict is only recorded: the node's compile is never
//! delayed and never waits for it; if the one dry-run slot or the memory budget
//! is busy the module is skipped and counted) and `enforce` (the worker judges
//! first and a refused module is refused). Linux only.
//!
//! Only deterministic verdicts are remembered as refusals: the module is invalid,
//! or it passed a cap that the kernel enforced and the worker or the kernel
//! reported (a refusal for memory is remembered per granted cap, since the cap
//! follows the node's headroom). A crash that cannot be attributed to the module,
//! a timeout, an outside kill, a worker that could not install its limits or
//! start, and a lack of memory are transient: nothing is cached as a refusal. A
//! module whose own check times out or crashes is not retried for a minute,
//! doubling to thirty. Outcomes that say nothing about the module (an outside
//! kill, a failed start, limits not installed, no headroom) open a global breaker
//! instead: every worker start is refused transiently for one second, doubling to
//! a minute, and the first answer closes it. A compile waits at most ten seconds
//! for a slot or for memory.
//!
//! Memory: every worker, and every in-process compile of a large module that the
//! worker accepted earlier, reserves against one budget (`budget`), so together
//! with the node's own resident memory they stay under 90% of the service limit.
//!
//! If the startup probe finds that no worker can run here, the node stays
//! available with isolation off for the run, and says so: an error log at most
//! once a minute while compiles go unchecked, the `compile_isolation_degraded`
//! gauge, and a banner on the node's status page. The probe judges the node at
//! the larger of its current memory and 1.5 GiB, so `enforce` needs a service
//! `MemoryMax` of at least 4 GiB, or none. At runtime, a compile refused for want
//! of headroom raises its own gauge (`compile_isolation_refusing_for_memory`) and
//! banner, which say that isolation is on and compiles are refused for now (cleared
//! by the next grant), and logs at most once a minute; in `enforce` it is refused
//! transiently.
//!
//! Which compiles are covered: contract code always, when the mode is on;
//! delegate code only when the node runs in hosted mode, and in dry-run on
//! every node, for measurement.

#[cfg(target_os = "linux")]
pub(crate) mod accepted;
#[cfg(target_os = "linux")]
pub(crate) mod budget;
#[cfg(target_os = "linux")]
pub(crate) mod caps;
#[cfg(target_os = "linux")]
mod isolation;
pub(crate) mod metrics;
#[cfg(target_os = "linux")]
pub(crate) mod protocol;
#[cfg(target_os = "linux")]
pub(crate) mod supervisor;
#[cfg(target_os = "linux")]
pub(crate) mod worker;

#[cfg(all(test, target_os = "linux"))]
pub(crate) use budget::HostLimits;
#[cfg(not(target_os = "linux"))]
pub(crate) use inert::Isolation;
#[cfg(target_os = "linux")]
pub(crate) use isolation::{Isolation, IsolationConfig};

/// Entry point for the node binary. If the process was started as a compile
/// worker (`freenet __compile-worker --parent-pid N`) this runs the worker and
/// never returns; otherwise it returns immediately. Call it first in `main`,
/// before any other work. Off Linux there is no worker and this does nothing.
pub fn run_compile_worker_if_requested() {
    #[cfg(target_os = "linux")]
    if std::env::args_os().nth(1).as_deref() == Some(std::ffi::OsStr::new(worker::WORKER_ARG)) {
        worker::worker_main_from_args();
    }
}

/// For the node binary's integration tests only: run the production worker (the
/// given `freenet` binary, started with its hidden argument, the real handshake
/// and the default caps) on `code`. `Ok` when it accepted the module, else a
/// description of what it did. Not part of the supported API.
#[doc(hidden)]
#[cfg(target_os = "linux")]
pub fn production_worker_accepts_for_tests(
    exe: &std::path::Path,
    code: &[u8],
) -> Result<(), String> {
    use supervisor::{Launcher, Verdict, classify, run_worker_once};
    let engine =
        crate::wasm_runtime::engine::wasmtime_engine::WasmtimeEngine::create_worker_engine(false)
            .map_err(|e| format!("engine: {e}"))?;
    let identity = protocol::Identity {
        engine_compat_id: crate::wasm_runtime::engine::wasmtime_engine::engine_compat_id(&engine),
        build_id: worker::build_id(),
        flags: 0,
    };
    let policy = caps::Policy::default();
    let caps = protocol::Caps {
        cpu_ms: policy.cpu_ms,
        data_bytes: policy.data_ceiling,
        wall_ms: policy.wall_ms,
    };
    let launcher = Launcher::production_at(exe.to_path_buf());
    let report = run_worker_once(&launcher, &identity, &caps, code).map_err(|e| e.0)?;
    match classify(&report, &caps) {
        Verdict::Accepted { .. } => Ok(()),
        other @ (Verdict::Refused(_)
        | Verdict::Crashed
        | Verdict::Timeout
        | Verdict::Interrupted
        | Verdict::Busy(_)) => Err(format!("{other:?}; {report:?}")),
    }
}

/// Which kind of code is being compiled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CodeKind {
    Contract,
    Delegate,
}

/// What other platforms get: the same interface, no worker. The scan still
/// runs; only the bounded check is absent.
#[cfg(not(target_os = "linux"))]
mod inert {
    use std::sync::Arc;

    use super::CodeKind;
    use super::metrics::CompileMetrics;
    use crate::config::CompileIsolationMode;
    use crate::wasm_runtime::engine::WasmError;
    use crate::wasm_runtime::engine::compile_verdicts::CompileVerdictCache;

    pub(crate) struct Isolation {
        metrics: Arc<CompileMetrics>,
        isolate_delegates: bool,
    }

    pub(crate) struct Ticket;

    impl Ticket {
        pub(crate) fn finish(self, _compiled: bool) {}
    }

    impl Isolation {
        pub(crate) fn inert(
            mode: CompileIsolationMode,
            isolate_delegates: bool,
            metrics: Arc<CompileMetrics>,
        ) -> Self {
            if mode != CompileIsolationMode::Off {
                tracing::warn!(
                    "compile-isolation is only available on Linux; it is ignored on this platform"
                );
            }
            Self {
                metrics,
                isolate_delegates,
            }
        }

        /// See the Linux `Isolation::coarse_check_applies`.
        pub(crate) fn coarse_check_applies(&self, kind: CodeKind) -> bool {
            match kind {
                CodeKind::Contract => true,
                CodeKind::Delegate => self.isolate_delegates,
            }
        }

        pub(crate) fn metrics(&self) -> &Arc<CompileMetrics> {
            &self.metrics
        }

        pub(crate) fn admit(
            &self,
            _input_hash: &[u8; 32],
            _code: &[u8],
            _kind: CodeKind,
            _verdicts: &CompileVerdictCache,
        ) -> Result<Ticket, WasmError> {
            Ok(Ticket)
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
pub(crate) mod tests;
