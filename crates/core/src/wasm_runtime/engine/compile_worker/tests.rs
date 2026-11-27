//! Process-level tests of the compile worker. The test binary plays the worker:
//! the supervisor starts it with a filter that selects [`worker_entry`], which
//! becomes the worker when its environment says so and passes trivially otherwise.
//!
//! These tests need Linux (`/proc`, `prctl`, `RLIMIT_DATA`) and a kernel that
//! enforces `RLIMIT_DATA` on `mmap` (4.7 or later); on any other system the
//! module is compiled out and nothing here runs.

pub(crate) const PARENT_ENV: &str = "FNCW_TEST_PARENT";
pub(crate) const WORKER_ENTRY_TEST: &str =
    "wasm_runtime::engine::compile_worker::tests::worker_entry";

/// Not a test of anything on its own: it is the worker's entry point when the
/// supervisor tests start this binary as one.
#[test]
fn worker_entry() {
    #[cfg(target_os = "linux")]
    if let Some(parent) = std::env::var(PARENT_ENV)
        .ok()
        .and_then(|v| v.parse::<i32>().ok())
    {
        super::worker::run_worker(parent);
    }
}

#[cfg(target_os = "linux")]
mod process {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use super::super::budget::LimitsSource;
    use super::super::caps::Policy;
    use super::super::metrics::{CompileMetrics, Outcome};
    use super::super::protocol::{Caps, Identity};
    use super::super::supervisor::{
        BusyReason, End, Launcher, RunReport, Verdict, classify, run_worker_once,
    };
    use super::super::worker;
    use super::super::{CodeKind, HostLimits, Isolation, IsolationConfig};
    use crate::config::CompileIsolationMode;
    use crate::wasm_runtime::engine::compile_limits::CompileRefusal;
    use crate::wasm_runtime::engine::compile_verdicts::CompileVerdictCache;
    use crate::wasm_runtime::engine::wasmtime_engine::{WasmtimeEngine, engine_compat_id};
    use crate::wasm_runtime::engine::{WasmEngine, WasmError};

    const MIB: u64 = 1 << 20;
    const GIB: u64 = 1 << 30;

    /// The node status flag is one process-wide value; tests that set it and
    /// read it back take this lock so they do not see each other's writes.
    static STATUS_FLAG: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn engine_id() -> u64 {
        engine_compat_id(&WasmtimeEngine::create_worker_engine(false).expect("engine"))
    }

    fn identity() -> Identity {
        Identity {
            engine_compat_id: engine_id(),
            build_id: worker::build_id(),
            flags: 0,
        }
    }

    fn caps(cpu_ms: u64, data: u64, wall_ms: u64) -> Caps {
        Caps {
            cpu_ms,
            data_bytes: data,
            wall_ms,
        }
    }

    fn run(fault: Option<&str>, caps: Caps, code: &[u8]) -> (RunReport, Verdict) {
        let l = Launcher::for_tests(fault);
        let r = run_worker_once(&l, &identity(), &caps, code).expect("the worker starts");
        let v = classify(&r, &caps);
        (r, v)
    }

    fn valid() -> Vec<u8> {
        wat::parse_str("(module (func (export \"f\") (result i32) (i32.const 1)))").unwrap()
    }

    fn valid_variant(n: u8) -> Vec<u8> {
        wat::parse_str(format!(
            "(module (func (export \"f\") (result i32) (i32.const {n})))"
        ))
        .unwrap()
    }

    fn assert_parent_healthy() {
        let (_, v) = run(None, caps(20_000, GIB, 60_000), &valid());
        assert!(matches!(v, Verdict::Accepted { .. }), "{v:?}");
    }

    #[test]
    fn a_valid_module_is_accepted_and_the_worker_reports_itself() {
        let (r, v) = run(None, caps(20_000, GIB, 60_000), &valid());
        let Verdict::Accepted { vm_hwm_kib, .. } = v else {
            panic!("{v:?} {r:?}");
        };
        assert!(vm_hwm_kib > 0, "the worker reports its own peak memory");
        assert_eq!(r.end, End::Exited(0));
    }

    #[test]
    fn text_and_invalid_modules_are_refused() {
        let (_, v) = run(None, caps(20_000, GIB, 60_000), b"(module)");
        assert!(
            matches!(v, Verdict::Refused(CompileRefusal::Malformed(_))),
            "{v:?}"
        );
        let (_, v) = run(
            None,
            caps(20_000, GIB, 60_000),
            &[0, 0x61, 0x73, 0x6d, 1, 0, 0, 0, 0xff],
        );
        assert!(
            matches!(v, Verdict::Refused(CompileRefusal::Malformed(_))),
            "{v:?}"
        );
    }

    #[test]
    fn a_mismatched_engine_identity_is_not_a_verdict_on_the_module() {
        let mut id = identity();
        id.engine_compat_id ^= 1;
        let l = Launcher::for_tests(None);
        let c = caps(20_000, GIB, 60_000);
        let r = run_worker_once(&l, &id, &c, &valid()).unwrap();
        assert_eq!(classify(&r, &c), Verdict::Busy(BusyReason::VersionMismatch));
        let mut id = identity();
        id.build_id[0] ^= 1;
        let r = run_worker_once(&l, &id, &c, &valid()).unwrap();
        assert_eq!(classify(&r, &c), Verdict::Busy(BusyReason::VersionMismatch));
    }

    #[test]
    fn the_worker_engine_matches_the_nodes_and_has_no_cache() {
        // Same configuration function, so the same identity as a node engine.
        let node = WasmtimeEngine::new(&crate::wasm_runtime::RuntimeConfig::default(), false)
            .expect("node engine");
        assert_eq!(engine_id(), node.engine_identity_for_tests());
    }

    #[test]
    fn allocating_to_the_cap_is_over_memory_and_the_parent_is_unharmed() {
        let started = Instant::now();
        let (r, v) = run(Some("alloc"), caps(20_000, 256 * MIB, 60_000), &valid());
        assert_eq!(v, Verdict::Refused(CompileRefusal::OverMemory), "{r:?}");
        assert!(started.elapsed() < Duration::from_secs(40));
        assert_parent_healthy();
    }

    #[test]
    fn a_burst_of_allocation_from_four_threads_stays_under_the_cap() {
        let (r, v) = run(Some("burst"), caps(60_000, 256 * MIB, 60_000), &valid());
        assert!(
            matches!(v, Verdict::Refused(CompileRefusal::OverMemory)),
            "{v:?} {r:?}"
        );
        assert_parent_healthy();
    }

    #[test]
    fn spinning_past_the_cpu_cap_is_over_cpu() {
        let started = Instant::now();
        let (r, v) = run(Some("spin"), caps(1_000, GIB, 60_000), &valid());
        assert_eq!(v, Verdict::Refused(CompileRefusal::OverCpu), "{r:?}");
        // Four threads reach one CPU-second quickly; the hard limit follows a
        // second after the soft one.
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "{:?}",
            started.elapsed()
        );
    }

    #[test]
    fn spinning_until_the_wall_timer_is_killed_and_is_a_timeout() {
        let started = Instant::now();
        // One spinning thread, a CPU cap far above the wall limit.
        let (r, v) = run(Some("spin1"), caps(600_000, GIB, 1_500), &valid());
        assert_eq!(v, Verdict::Timeout, "{r:?}");
        assert!(r.killed_by_us);
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "{:?}",
            started.elapsed()
        );
        assert_parent_healthy();
    }

    #[test]
    fn sleeping_is_a_timeout_not_a_cpu_refusal() {
        let (r, v) = run(Some("sleep"), caps(20_000, GIB, 1_000), &valid());
        assert_eq!(v, Verdict::Timeout, "{r:?}");
    }

    #[test]
    fn crashes_are_transient_and_do_not_affect_the_next_compile() {
        for fault in ["abort", "segv", "stack", "panic", "exit-silent", "exit-7"] {
            let (r, v) = run(Some(fault), caps(20_000, GIB, 60_000), &valid());
            assert_eq!(v, Verdict::Crashed, "{fault}: {r:?}");
        }
        assert_parent_healthy();
    }

    #[test]
    fn garbage_output_is_no_verdict() {
        let (r, v) = run(Some("garbage"), caps(20_000, GIB, 60_000), &valid());
        assert_eq!(v, Verdict::Busy(BusyReason::ProtocolError), "{r:?}");
    }

    #[test]
    fn stderr_and_stdout_floods_are_bounded() {
        let started = Instant::now();
        let (r, _) = run(Some("stderr-flood"), caps(20_000, GIB, 60_000), &valid());
        assert!(
            r.stderr_tail.len() <= 4096 + 8192,
            "{}",
            r.stderr_tail.len()
        );
        let (_r, _v) = run(Some("huge-reply"), caps(20_000, GIB, 60_000), &valid());
        assert!(started.elapsed() < Duration::from_secs(60));
    }

    #[test]
    fn the_worker_starts_with_an_empty_environment_and_no_inherited_descriptors() {
        let (r, _) = run(Some("report-env"), caps(20_000, GIB, 60_000), &valid());
        let text = r.response.expect("replied").reason;
        // Only what the test launcher itself passes.
        assert_eq!(text, "env=1");
        let (r, _) = run(Some("report-fds"), caps(20_000, GIB, 60_000), &valid());
        let text = r.response.expect("replied").reason;
        let nums: Vec<u32> = text
            .trim_start_matches("fds=[")
            .trim_end_matches(']')
            .split(", ")
            .filter_map(|n| n.parse().ok())
            .collect();
        // 0, 1, 2 and at most the descriptor used to list the directory.
        assert!(nums.iter().all(|n| *n <= 3), "{text}");
        assert!(
            nums.contains(&0) && nums.contains(&1) && nums.contains(&2),
            "{text}"
        );
    }

    #[test]
    fn a_worker_whose_parent_is_not_the_named_one_exits_at_once() {
        let l = Launcher::for_tests(Some("sleep")).with_parent_pid(1_999_999);
        let c = caps(20_000, GIB, 60_000);
        let started = Instant::now();
        let r = run_worker_once(&l, &identity(), &c, &valid()).unwrap();
        assert_eq!(r.end, End::Exited(3), "{r:?}");
        assert!(started.elapsed() < Duration::from_secs(20));
        assert_eq!(classify(&r, &c), Verdict::Busy(BusyReason::ProtocolError));
    }

    /// The test binary also plays a short-lived parent: it starts a sleeping worker
    /// and dies without waiting. Used by the parent-death test.
    // The child is left running on purpose: this process dies without waiting, and
    // the test that started it checks that the child follows.
    #[allow(clippy::zombie_processes)]
    #[test]
    fn mid_parent_entry() {
        let Ok(path) = std::env::var("FNCW_TEST_PIDFILE") else {
            return;
        };
        let l = Launcher::for_tests(Some("sleep"));
        let child = l.command().spawn().expect("spawn worker");
        std::fs::write(&path, child.id().to_string()).unwrap();
        // Give the worker time to arm its parent-death signal, then die abruptly.
        std::thread::sleep(Duration::from_millis(1500));
        // SAFETY: immediate process exit without destructors, as a crash would.
        unsafe { libc::_exit(0) };
    }

    #[test]
    fn a_worker_dies_when_its_parent_dies() {
        let dir = std::env::temp_dir().join(format!("fncw-pd-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let pidfile = dir.join("pid");
        let mut mid = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "wasm_runtime::engine::compile_worker::tests::process::mid_parent_entry",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("FNCW_TEST_PIDFILE", &pidfile)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(60);
        let pid = loop {
            if let Ok(t) = std::fs::read_to_string(&pidfile) {
                if let Ok(p) = t.trim().parse::<i32>() {
                    break p;
                }
            }
            assert!(
                Instant::now() < deadline,
                "the intermediate parent did not start a worker"
            );
            std::thread::sleep(Duration::from_millis(50));
        };
        // SAFETY: signal 0 only tests for existence.
        let alive = |p: i32| unsafe { libc::kill(p, 0) } == 0;
        assert!(
            alive(pid),
            "the worker should be running while its parent lives"
        );
        drop(mid.wait());
        let deadline = Instant::now() + Duration::from_secs(20);
        while alive(pid) {
            // A dead worker is reaped by init once its parent is gone; a zombie
            // still answers kill(0), so also accept the zombie state.
            let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
            if stat.contains(") Z") || stat.is_empty() {
                break;
            }
            assert!(Instant::now() < deadline, "the worker outlived its parent");
            std::thread::sleep(Duration::from_millis(100));
        }
        drop(std::fs::remove_dir_all(&dir));
    }

    // -----------------------------------------------------------------------
    // The decision layer: modes, kinds, caching, the accepted set, the probe.
    // -----------------------------------------------------------------------

    fn limits() -> LimitsSource {
        Arc::new(|| HostLimits {
            memory_limit: 64 * GIB,
            node_rss: 0,
            cores: None,
            limit_unreadable: false,
        })
    }

    fn small_policy() -> Policy {
        Policy {
            cpu_ms: 20_000,
            wall_ms: 60_000,
            data_floor: 256 * MIB,
            data_ceiling: 256 * MIB,
            max_gate: 2,
        }
    }

    fn iso(
        mode: CompileIsolationMode,
        hosted: bool,
        fault: Option<&str>,
    ) -> (Isolation, Arc<CompileMetrics>) {
        iso_with(mode, hosted, Launcher::for_tests(fault), small_policy())
    }

    fn iso_with(
        mode: CompileIsolationMode,
        hosted: bool,
        launcher: Launcher,
        policy: Policy,
    ) -> (Isolation, Arc<CompileMetrics>) {
        iso_on(mode, hosted, launcher, policy, limits())
    }

    fn iso_on(
        mode: CompileIsolationMode,
        hosted: bool,
        launcher: Launcher,
        policy: Policy,
        host: LimitsSource,
    ) -> (Isolation, Arc<CompileMetrics>) {
        let metrics = Arc::new(CompileMetrics::default());
        let i = Isolation::build(
            &IsolationConfig {
                mode,
                isolate_delegates: hosted,
                accepted_dir: None,
                consume_fuel: false,
            },
            engine_id(),
            Some(launcher),
            policy,
            host,
            metrics.clone(),
        );
        (i, metrics)
    }

    fn compile_err(r: Result<super::super::isolation::Ticket, WasmError>) -> WasmError {
        match r {
            Err(e) => e,
            Ok(_) => panic!("expected the compile to be refused"),
        }
    }

    fn verdict_cache() -> CompileVerdictCache {
        CompileVerdictCache::new(
            Arc::new(crate::util::time_source::InstantTimeSrc::new()),
            engine_id(),
        )
    }

    fn count(m: &CompileMetrics, o: Outcome) -> u64 {
        m.snapshot().outcome(o)
    }

    fn hash(code: &[u8]) -> [u8; 32] {
        *blake3::hash(code).as_bytes()
    }

    #[test]
    fn enforce_refuses_contract_code_the_worker_refuses_and_remembers_it() {
        let (i, m) = iso(CompileIsolationMode::Enforce, false, Some("alloc"));
        let v = verdict_cache();
        let code = valid();
        let first = i.admit(&hash(&code), &code, CodeKind::Contract, &v);
        assert!(
            matches!(
                first,
                Err(WasmError::CompileRefused(CompileRefusal::OverMemory))
            ),
            "{:?}",
            first.err()
        );
        let second = i.admit(&hash(&code), &code, CodeKind::Contract, &v);
        assert!(matches!(
            second,
            Err(WasmError::CompileRefused(CompileRefusal::OverMemory))
        ));
        assert_eq!(
            count(&m, Outcome::OverMemory),
            1,
            "the second answer came from the cache"
        );
    }

    #[test]
    fn a_crash_is_transient_not_cached_and_the_module_backs_off() {
        let (i, m) = iso(CompileIsolationMode::Enforce, false, Some("abort"));
        let v = verdict_cache();
        let a = valid();
        let mut b = valid();
        b.extend_from_slice(&[0, 1, 0]); // a custom-section-like trailing change
        let first = compile_err(i.admit(&hash(&a), &a, CodeKind::Contract, &v));
        assert!(matches!(first, WasmError::Compile(_)), "{first:?}");
        assert!(
            v.get(&hash(&a)).is_none(),
            "a crash is not a remembered refusal"
        );
        assert!(i.in_backoff_for_tests(&hash(&a)));
        // Asked again, the same bytes cost no worker.
        let again = compile_err(i.admit(&hash(&a), &a, CodeKind::Contract, &v));
        assert!(matches!(again, WasmError::Compile(_)));
        assert_eq!(count(&m, Outcome::Crashed), 1);
        assert_eq!(count(&m, Outcome::BackoffSkip), 1);
        // A changed byte is another module and costs one more worker.
        let other = compile_err(i.admit(&hash(&b), &b, CodeKind::Contract, &v));
        assert!(matches!(other, WasmError::Compile(_)));
        assert_eq!(count(&m, Outcome::Crashed), 2);
    }

    #[test]
    fn the_backoff_doubles_to_a_ceiling_and_the_map_stays_bounded() {
        let (i, _) = iso(CompileIsolationMode::Enforce, false, None);
        let h = [7u8; 32];
        let mut last = Duration::ZERO;
        for n in 1..=12u32 {
            i.note_failure_for_tests(h);
            let left = i.backoff_left_for_tests(&h).expect("in back-off");
            // 60 s doubling to 30 min, with the +-20% jitter of the shared tracker.
            let nominal = Duration::from_secs(60 * (1u64 << (n - 1).min(10)))
                .min(Duration::from_secs(30 * 60));
            assert!(
                left <= nominal.mul_f64(1.2)
                    && left + Duration::from_secs(5) >= nominal.mul_f64(0.8),
                "failure {n}: {left:?} vs {nominal:?}"
            );
            last = left;
        }
        assert!(last > Duration::from_secs(23 * 60) && last <= Duration::from_secs(36 * 60));
        for n in 0..2_000u32 {
            let mut k = [0u8; 32];
            k[..4].copy_from_slice(&n.to_le_bytes());
            i.note_failure_for_tests(k);
        }
        assert!(
            i.backoff_len_for_tests() <= 256,
            "{}",
            i.backoff_len_for_tests()
        );
    }

    #[test]
    fn dry_run_never_changes_the_outcome_but_records_what_would_happen() {
        for fault in [
            "alloc-big",
            "abort-big",
            "sleep-big",
            "exit-silent",
            "garbage",
        ] {
            let mut p = small_policy();
            p.wall_ms = 1_500;
            let (i, m) = iso_with(
                CompileIsolationMode::DryRun,
                false,
                Launcher::for_tests(Some(fault)),
                p,
            );
            let v = verdict_cache();
            let code = valid();
            let r = i.admit(&hash(&code), &code, CodeKind::Contract, &v);
            assert!(r.is_ok(), "{fault}: dry-run must not refuse: {:?}", r.err());
            i.wait_for_dry_runs();
            assert!(v.get(&hash(&code)).is_none());
            if fault != "garbage" {
                assert_eq!(count(&m, Outcome::WouldRefuse), 1, "{fault}");
            }
        }
    }

    /// Dry-run is behaviour-neutral: the node's compile is released at once and
    /// holds nothing of the worker's, whatever the worker does.
    #[test]
    fn dry_run_returns_at_once_and_a_busy_slot_skips_the_module() {
        let mut p = small_policy();
        p.wall_ms = 2_000;
        let (i, m) = iso_with(
            CompileIsolationMode::DryRun,
            false,
            Launcher::for_tests(Some("sleep")),
            p,
        );
        let v = verdict_cache();
        let a = valid();
        let mut b = valid();
        b.extend_from_slice(&[0, 1, 0]);
        let started = Instant::now();
        let t1 = i.admit(&hash(&a), &a, CodeKind::Contract, &v).unwrap();
        let t2 = i.admit(&hash(&b), &b, CodeKind::Contract, &v).unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(1_000),
            "dry-run delayed the compile: {:?}",
            started.elapsed()
        );
        // The first holds the one dry-run slot; the second is skipped and counted.
        assert_eq!(count(&m, Outcome::DryRunSkipped), 1);
        // The tickets hold no memory: the node's compile owns nothing of the worker's.
        assert_eq!(i.budget().reserved(), small_policy().data_ceiling);
        t1.finish(true);
        t2.finish(true);
        i.wait_for_dry_runs();
        assert_eq!(i.budget().reserved(), 0);
        assert_eq!(count(&m, Outcome::Timeout), 1);
    }

    #[test]
    fn dry_run_accepts_and_counts_an_accepted_module() {
        let (i, m) = iso(CompileIsolationMode::DryRun, false, None);
        let v = verdict_cache();
        let code = valid();
        let t = i
            .admit(&hash(&code), &code, CodeKind::Contract, &v)
            .unwrap();
        t.finish(true);
        i.wait_for_dry_runs();
        assert_eq!(count(&m, Outcome::Accepted), 1);
        assert_eq!(i.accepted_len(), 1);
        // The next request for the same module skips the worker.
        let t = i
            .admit(&hash(&code), &code, CodeKind::Contract, &v)
            .unwrap();
        t.finish(true);
        assert_eq!(count(&m, Outcome::AcceptedSkip), 1);
        assert_eq!(count(&m, Outcome::Accepted), 1);
        let snap = m.snapshot();
        assert_eq!(
            (
                snap.accepted_set_size,
                snap.module_bytes.iter().sum::<u64>()
            ),
            (1, 2)
        );
    }

    #[test]
    fn a_module_the_node_could_not_compile_is_not_remembered_as_accepted() {
        let (i, _) = iso(CompileIsolationMode::Enforce, false, None);
        let v = verdict_cache();
        let code = valid();
        let t = i
            .admit(&hash(&code), &code, CodeKind::Contract, &v)
            .unwrap();
        t.finish(false);
        assert_eq!(i.accepted_len(), 0);
    }

    #[test]
    fn off_runs_nothing() {
        let (i, m) = iso(CompileIsolationMode::Off, true, Some("abort"));
        let v = verdict_cache();
        let code = valid();
        for kind in [CodeKind::Contract, CodeKind::Delegate] {
            i.admit(&hash(&code), &code, kind, &v).unwrap().finish(true);
        }
        assert!(Outcome::ALL.iter().all(|o| count(&m, *o) == 0));
    }

    #[test]
    fn delegates_go_through_the_worker_only_on_hosted_nodes_when_enforcing() {
        let code = valid();
        // Not hosted: delegate code is not checked at all.
        let (i, m) = iso(CompileIsolationMode::Enforce, false, Some("abort"));
        let v = verdict_cache();
        assert!(i.admit(&hash(&code), &code, CodeKind::Delegate, &v).is_ok());
        assert!(
            Outcome::ALL.iter().all(|o| count(&m, *o) == 0),
            "no worker ran"
        );
        // Hosted: the same delegate is checked, and a worker that dies on it gives
        // no verdict (transient).
        let (i, m) = iso(CompileIsolationMode::Enforce, true, Some("abort"));
        let r = compile_err(i.admit(&hash(&code), &code, CodeKind::Delegate, &v));
        assert!(matches!(r, WasmError::Compile(_)), "{r:?}");
        assert_eq!(count(&m, Outcome::Crashed), 1);
    }

    #[test]
    fn delegates_are_measured_in_dry_run_on_every_node() {
        let code = valid();
        let (i, m) = iso(CompileIsolationMode::DryRun, false, Some("abort"));
        let v = verdict_cache();
        assert!(i.admit(&hash(&code), &code, CodeKind::Delegate, &v).is_ok());
        i.wait_for_dry_runs();
        assert_eq!(count(&m, Outcome::WouldRefuse), 1);
    }

    #[test]
    fn a_timeout_is_transient_and_the_same_module_backs_off() {
        let mut p = small_policy();
        p.wall_ms = 800;
        let (i, metrics) = iso_with(
            CompileIsolationMode::Enforce,
            false,
            Launcher::for_tests(Some("sleep")),
            p,
        );
        let v = verdict_cache();
        let code = valid();
        let r = compile_err(i.admit(&hash(&code), &code, CodeKind::Contract, &v));
        assert!(matches!(r, WasmError::Compile(_)), "{r:?}");
        assert!(
            v.get(&hash(&code)).is_none(),
            "a timeout is not a remembered refusal"
        );
        assert_eq!(count(&metrics, Outcome::Timeout), 1);
        // The next answer comes without running a worker.
        let r = compile_err(i.admit(&hash(&code), &code, CodeKind::Contract, &v));
        assert!(matches!(r, WasmError::Compile(_)));
        assert_eq!(count(&metrics, Outcome::Timeout), 1);
        assert_eq!(count(&metrics, Outcome::BackoffSkip), 1);
    }

    #[test]
    fn when_no_worker_can_start_enforce_refuses_transiently_and_never_compiles_unchecked() {
        let (i, metrics) = iso_with(
            CompileIsolationMode::Enforce,
            false,
            Launcher::missing_for_tests(),
            small_policy(),
        );
        let v = verdict_cache();
        let code = valid();
        let r = compile_err(i.admit(&hash(&code), &code, CodeKind::Contract, &v));
        assert!(matches!(r, WasmError::Compile(_)), "{r:?}");
        assert!(v.get(&hash(&code)).is_none());
        assert_eq!(count(&metrics, Outcome::SpawnFailed), 1);
    }

    #[test]
    fn a_failing_startup_probe_keeps_the_node_available_and_says_so() {
        let _status = STATUS_FLAG.lock().unwrap_or_else(|e| e.into_inner());
        let (i, metrics) = iso_with(
            CompileIsolationMode::Enforce,
            false,
            Launcher::missing_for_tests(),
            small_policy(),
        );
        i.probe();
        assert_eq!(count(&metrics, Outcome::ProbeFailed), 1);
        assert!(i.is_degraded());
        // The gauge, for telemetry, and the node status flag.
        assert_eq!(metrics.snapshot().telemetry().compile_isolation_degraded, 1);
        assert!(crate::node::network_status::compile_isolation_degraded());
        let v = verdict_cache();
        let code = valid();
        // Off for the run: the module goes to the node's own compile, counted.
        assert!(i.admit(&hash(&code), &code, CodeKind::Contract, &v).is_ok());
        assert!(i.admit(&hash(&code), &code, CodeKind::Contract, &v).is_ok());
        assert!(count(&metrics, Outcome::Degraded) >= 2);
        // A probe that works leaves it on.
        let (i, m) = iso(CompileIsolationMode::Enforce, false, None);
        i.probe();
        assert_eq!(count(&m, Outcome::ProbeFailed), 0);
        assert!(!i.is_degraded());
        assert_eq!(m.snapshot().telemetry().compile_isolation_degraded, 0);
    }

    /// Less than the floor under the service limit means no worker can ever run:
    /// that is a probe failure, not a node that refuses every new contract.
    #[test]
    fn too_little_memory_at_the_probe_is_a_probe_failure() {
        let (i, metrics) = iso_on(
            CompileIsolationMode::Enforce,
            false,
            Launcher::for_tests(None),
            small_policy(),
            // A 2 GiB unit whose node is already at 1.7 GiB.
            Arc::new(|| HostLimits {
                memory_limit: 2 * GIB,
                node_rss: GIB * 17 / 10,
                cores: None,
                limit_unreadable: false,
            }),
        );
        i.probe();
        assert_eq!(count(&metrics, Outcome::ProbeFailed), 1);
        assert!(i.is_degraded());
    }

    /// The probe judges the node at its expected size (at least 1.5 GiB), not at
    /// the small size of a node that has just started: a 2 GiB unit cannot hold a
    /// grown node and the floor of a worker, so it is marked degraded at startup
    /// instead of flapping later; a 4 GiB unit passes.
    #[test]
    fn the_probe_judges_a_small_unit_at_the_nodes_expected_size() {
        let _status = STATUS_FLAG.lock().unwrap_or_else(|e| e.into_inner());
        let at = |limit: u64| {
            let (i, m) = iso_on(
                CompileIsolationMode::Enforce,
                false,
                Launcher::for_tests(None),
                Policy::default(),
                // A node that has just started.
                Arc::new(move || HostLimits {
                    memory_limit: limit,
                    node_rss: 300 * MIB,
                    cores: None,
                    limit_unreadable: false,
                }),
            );
            i.probe();
            (i.is_degraded(), count(&m, Outcome::ProbeFailed))
        };
        assert_eq!(at(2 * GIB), (true, 1));
        assert_eq!(at(4 * GIB), (false, 0));
    }

    /// Too little headroom at runtime is reported, not only counted: the gauge and
    /// banner go up, the refusal is transient and not remembered, and the next
    /// grant clears the report.
    #[test]
    fn too_little_memory_under_the_service_limit_is_a_transient_refusal_and_is_reported() {
        let _status = STATUS_FLAG.lock().unwrap_or_else(|e| e.into_inner());
        // The startup-probe test leaves the process-wide fail-open flag set.
        crate::node::network_status::set_compile_isolation_degraded(false);
        crate::node::network_status::set_compile_isolation_refusing_for_memory(false);
        let rss = Arc::new(std::sync::atomic::AtomicU64::new(GIB * 8 / 5));
        let rss2 = rss.clone();
        let (i, metrics) = iso_on(
            CompileIsolationMode::Enforce,
            false,
            Launcher::for_tests(None),
            small_policy(),
            // A 2 GiB unit whose node is at 1.6 GiB right now.
            Arc::new(move || HostLimits {
                memory_limit: 2 * GIB,
                node_rss: rss2.load(std::sync::atomic::Ordering::SeqCst),
                cores: None,
                limit_unreadable: false,
            }),
        );
        let v = verdict_cache();
        let code = valid();
        let r = compile_err(i.admit(&hash(&code), &code, CodeKind::Contract, &v));
        assert!(matches!(r, WasmError::Compile(_)));
        assert_eq!(count(&metrics, Outcome::Busy), 1);
        // Isolation stays on: only the refusing-for-memory state is raised, never
        // the startup fail-open one (which says compiles are unchecked).
        let t = metrics.snapshot().telemetry();
        assert_eq!(t.compile_isolation_refusing_for_memory, 1);
        assert_eq!(t.compile_isolation_degraded, 0);
        assert!(crate::node::network_status::compile_isolation_refusing_for_memory());
        assert!(!crate::node::network_status::compile_isolation_degraded());
        assert!(v.get(&hash(&code)).is_none(), "not remembered");
        assert!(
            !i.in_backoff_for_tests(&hash(&code)),
            "the module is not at fault"
        );
        // The refusal opened the breaker: an immediate retry costs nothing.
        let again = compile_err(i.admit(&hash(&code), &code, CodeKind::Contract, &v));
        assert!(matches!(again, WasmError::Compile(_)));
        // The node's memory falls and the breaker's first second passes.
        rss.store(0, std::sync::atomic::Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(1_500));
        i.admit(&hash(&code), &code, CodeKind::Contract, &v)
            .expect("granted once there is headroom")
            .finish(true);
        assert_eq!(
            metrics
                .snapshot()
                .telemetry()
                .compile_isolation_refusing_for_memory,
            0
        );
        assert!(!crate::node::network_status::compile_isolation_refusing_for_memory());
    }

    /// A caller queued for a slot must recheck the global breaker once it has a
    /// reservation: callers waiting when the breaker opens start no worker.
    #[test]
    fn callers_queued_when_the_breaker_opens_start_no_worker() {
        let (i, metrics) = iso_with(
            CompileIsolationMode::Enforce,
            false,
            Launcher::missing_for_tests(),
            small_policy(),
        );
        // Both slots held, so every caller below waits in acquire_worker.
        let held: Vec<_> = (0..2)
            .map(|_| {
                i.budget()
                    .acquire_worker(Duration::from_secs(1))
                    .expect("slot")
            })
            .collect();
        let v = Arc::new(verdict_cache());
        let waiters: Vec<_> = (0..4u8)
            .map(|n| {
                let i = i.clone();
                let v = v.clone();
                std::thread::spawn(move || {
                    let code = valid_variant(n);
                    i.admit(&hash(&code), &code, CodeKind::Contract, &v).is_ok()
                })
            })
            .collect();
        std::thread::sleep(Duration::from_millis(400));
        i.breaker_trip_for_tests();
        drop(held);
        for w in waiters {
            assert!(!w.join().expect("join"), "refused during the cooldown");
        }
        assert_eq!(
            count(&metrics, Outcome::SpawnFailed),
            0,
            "no worker was launched"
        );
    }

    fn degraded_state_agrees(i: &Isolation) -> bool {
        let flag = i.refusing_for_memory_for_tests();
        flag == crate::node::network_status::compile_isolation_refusing_for_memory()
    }

    /// Interleave a degrade and a recovery: the recovery lands while the degrade
    /// is between its flag change and its publication. The published state must
    /// end up equal to the flag.
    #[test]
    fn headroom_transitions_cannot_leave_the_published_state_stale() {
        let _status = STATUS_FLAG.lock().unwrap_or_else(|e| e.into_inner());
        let (i, metrics) = iso(CompileIsolationMode::Enforce, false, None);
        let entered = Arc::new(std::sync::Barrier::new(2));
        let e2 = entered.clone();
        let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let f2 = fired.clone();
        super::super::isolation::tests_hook::set(
            i.id_for_tests(),
            Some(Arc::new(move || {
                if !f2.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    e2.wait();
                    std::thread::sleep(Duration::from_millis(300));
                }
            })),
        );
        let a = i.clone();
        let degrade = std::thread::spawn(move || a.headroom_degraded_for_tests(1));
        entered.wait();
        let b = i.clone();
        let recover = std::thread::spawn(move || b.headroom_recovered_for_tests());
        degrade.join().expect("join");
        recover.join().expect("join");
        super::super::isolation::tests_hook::set(i.id_for_tests(), None);
        assert!(degraded_state_agrees(&i));
        assert_eq!(
            metrics
                .snapshot()
                .telemetry()
                .compile_isolation_refusing_for_memory,
            u64::from(i.refusing_for_memory_for_tests())
        );
    }

    /// Many concurrent transitions end with the published state equal to the flag.
    #[test]
    fn concurrent_headroom_transitions_end_consistent() {
        let _status = STATUS_FLAG.lock().unwrap_or_else(|e| e.into_inner());
        let (i, _metrics) = iso(CompileIsolationMode::Enforce, false, None);
        let threads: Vec<_> = (0..8)
            .map(|n| {
                let i = i.clone();
                std::thread::spawn(move || {
                    for k in 0..200 {
                        if (n + k) % 2 == 0 {
                            i.headroom_degraded_for_tests(1);
                        } else {
                            i.headroom_recovered_for_tests();
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().expect("join");
        }
        assert!(degraded_state_agrees(&i));
    }

    /// Nothing about the environment is held against the module: a worker that
    /// cannot start opens the global breaker, so every module is refused
    /// transiently for a while, and no module is put in back-off.
    #[test]
    fn a_failed_spawn_opens_a_global_breaker_and_never_backs_off_the_module() {
        let (i, metrics) = iso_with(
            CompileIsolationMode::Enforce,
            false,
            Launcher::missing_for_tests(),
            small_policy(),
        );
        let v = verdict_cache();
        let a = valid();
        let mut b = valid();
        b.extend_from_slice(&[0, 1, 0]);
        let e = compile_err(i.admit(&hash(&a), &a, CodeKind::Contract, &v));
        assert!(matches!(e, WasmError::Compile(_)), "{e:?}");
        assert_eq!(count(&metrics, Outcome::SpawnFailed), 1);
        assert!(!i.in_backoff_for_tests(&hash(&a)));
        // Another module is refused without a start being attempted.
        let e = compile_err(i.admit(&hash(&b), &b, CodeKind::Contract, &v));
        assert!(matches!(e, WasmError::Compile(_)), "{e:?}");
        assert_eq!(count(&metrics, Outcome::SpawnFailed), 1, "no second start");
        assert_eq!(count(&metrics, Outcome::BackoffSkip), 0);
        assert_eq!(i.backoff_len_for_tests(), 0);
    }

    /// A refusal for memory is only valid at the cap it was made at.
    #[test]
    fn a_memory_refusal_is_remembered_per_granted_cap() {
        let rss = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let rss2 = rss.clone();
        let mut p = small_policy();
        p.data_floor = 200 * MIB;
        let (i, m) = iso_on(
            CompileIsolationMode::Enforce,
            false,
            Launcher::for_tests(Some("alloc")),
            p,
            Arc::new(move || HostLimits {
                memory_limit: GIB,
                node_rss: rss2.load(std::sync::atomic::Ordering::SeqCst),
                cores: None,
                limit_unreadable: false,
            }),
        );
        let v = verdict_cache();
        let code = valid();
        let over = |r: Result<_, WasmError>| {
            assert!(
                matches!(
                    r.err(),
                    Some(WasmError::CompileRefused(CompileRefusal::OverMemory))
                ),
                "expected OverMemory"
            );
        };
        over(
            i.admit(&hash(&code), &code, CodeKind::Contract, &v)
                .map(drop),
        );
        over(
            i.admit(&hash(&code), &code, CodeKind::Contract, &v)
                .map(drop),
        );
        assert_eq!(
            count(&m, Outcome::OverMemory),
            1,
            "same cap: from the cache"
        );
        // The node's memory moves, so the grant is smaller: a fresh judgement.
        rss.store(700 * MIB, std::sync::atomic::Ordering::SeqCst);
        over(
            i.admit(&hash(&code), &code, CodeKind::Contract, &v)
                .map(drop),
        );
        assert_eq!(
            count(&m, Outcome::OverMemory),
            2,
            "another cap: asked again"
        );
    }

    /// A worker that cannot install a limit it was asked for (here a hard limit
    /// inherited from the node, below the requested one) judged nothing: that is
    /// an infrastructure failure, never a refusal of the module.
    #[test]
    fn a_limit_that_cannot_be_installed_is_an_infrastructure_failure_not_a_refusal() {
        let l = Launcher::for_tests(None).with_inherited_cpu_hard_limit(10);
        let c = caps(20_000, GIB, 60_000);
        let r = run_worker_once(&l, &identity(), &c, &valid()).expect("the worker starts");
        assert_eq!(
            classify(&r, &c),
            Verdict::Busy(BusyReason::LimitsNotInstalled),
            "{r:?}"
        );
        // Through the decision layer: transient, counted as infrastructure, and
        // not remembered.
        let (i, m) = iso_with(CompileIsolationMode::Enforce, false, l, small_policy());
        let v = verdict_cache();
        let code = valid();
        let e = compile_err(i.admit(&hash(&code), &code, CodeKind::Contract, &v));
        assert!(matches!(e, WasmError::Compile(_)), "{e:?}");
        assert!(v.get(&hash(&code)).is_none());
        assert_eq!(count(&m, Outcome::InfraFailure), 1);
        assert_eq!(count(&m, Outcome::OverCpu), 0);
        assert!(
            !i.in_backoff_for_tests(&hash(&code)),
            "not the module's fault"
        );
        // Such a worker also fails the startup probe: isolation is off, loudly.
        i.probe();
        assert!(i.is_degraded());
    }

    /// A module over the large-module size, valid, that the worker accepts.
    fn large_valid() -> Vec<u8> {
        let mut m = valid();
        let payload = vec![0u8; (1 << 20) + 4096];
        // A custom section: id 0, size (LEB128), name length 1, name "p", payload.
        let mut body = vec![1u8, b'p'];
        body.extend_from_slice(&payload);
        m.push(0);
        let mut n = body.len();
        loop {
            let byte = (n & 0x7f) as u8;
            n >>= 7;
            if n == 0 {
                m.push(byte);
                break;
            }
            m.push(byte | 0x80);
        }
        m.extend_from_slice(&body);
        m
    }

    /// An accepted module is not free: its in-process compile still reserves
    /// memory from the same budget as a worker.
    #[test]
    fn a_module_skipping_the_worker_still_takes_the_memory_budget() {
        let (i, m) = iso(CompileIsolationMode::Enforce, false, None);
        let v = verdict_cache();
        let big = large_valid();
        let t = i.admit(&hash(&big), &big, CodeKind::Contract, &v).unwrap();
        assert_eq!(i.budget().reserved(), small_policy().data_ceiling);
        t.finish(true);
        assert_eq!(i.budget().reserved(), 0, "released with the ticket");
        // Second request: accepted, no worker, and the budget is still taken.
        let t = i.admit(&hash(&big), &big, CodeKind::Contract, &v).unwrap();
        assert_eq!(count(&m, Outcome::AcceptedSkip), 1);
        assert_eq!(count(&m, Outcome::Accepted), 1);
        assert_eq!(i.budget().reserved(), small_policy().data_floor);
        t.finish(true);
        assert_eq!(i.budget().reserved(), 0);
    }

    fn range_policy() -> Policy {
        Policy {
            data_floor: 100 * MIB,
            data_ceiling: 400 * MIB,
            ..small_policy()
        }
    }

    /// An accepted module of any size reserves what was recorded when it was
    /// accepted; a record with no figure reserves the ceiling.
    #[test]
    fn an_accepted_modules_recompile_reserves_its_recorded_requirement() {
        let (i, _) = iso_with(
            CompileIsolationMode::Enforce,
            false,
            Launcher::for_tests(None),
            range_policy(),
        );
        let v = verdict_cache();
        let small = valid();
        let big = large_valid();
        // Accepted by a worker: the figure comes from its reported peak.
        for code in [&small, &big] {
            i.admit(&hash(code), code, CodeKind::Contract, &v)
                .unwrap()
                .finish(true);
            let mib = i.requirement_for_tests(&hash(code)).expect("remembered");
            assert!(mib >= 1, "a worker's peak is recorded, not 'unknown'");
            let t = i.admit(&hash(code), code, CodeKind::Contract, &v).unwrap();
            let want = (u64::from(mib) * MIB).clamp(100 * MIB, 400 * MIB);
            assert_eq!(i.budget().reserved(), want, "{} bytes", code.len());
            t.finish(true);
            assert_eq!(i.budget().reserved(), 0);
        }
        // A recorded figure between floor and ceiling is reserved as it is.
        let mid = [7u8; 32];
        i.accept_for_tests(mid, 250);
        let t = i.admit(&mid, &small, CodeKind::Contract, &v).unwrap();
        assert_eq!(i.budget().reserved(), 250 * MIB);
        t.finish(true);
        // One below the floor reserves the floor.
        let low = [8u8; 32];
        i.accept_for_tests(low, 3);
        let t = i.admit(&low, &small, CodeKind::Contract, &v).unwrap();
        assert_eq!(i.budget().reserved(), 100 * MIB);
        t.finish(true);
    }

    #[test]
    fn an_accepted_record_without_a_figure_reserves_the_ceiling() {
        let (i, _) = iso_with(
            CompileIsolationMode::Enforce,
            false,
            Launcher::for_tests(None),
            range_policy(),
        );
        let v = verdict_cache();
        let small = valid();
        let legacy = [9u8; 32];
        i.accept_for_tests(legacy, 0);
        let t = i.admit(&legacy, &small, CodeKind::Contract, &v).unwrap();
        assert_eq!(i.budget().reserved(), 400 * MIB);
        t.finish(true);
        assert_eq!(i.budget().reserved(), 0);
    }

    /// Without the headroom for the recorded requirement the compile is a
    /// transient refusal and nothing is reserved, for a module of any size.
    #[test]
    fn insufficient_headroom_for_an_accepted_module_is_busy_and_reserves_nothing() {
        let rss = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let rss2 = rss.clone();
        let (i, m) = iso_on(
            CompileIsolationMode::Enforce,
            false,
            Launcher::for_tests(None),
            range_policy(),
            Arc::new(move || HostLimits {
                memory_limit: 2 * GIB,
                node_rss: rss2.load(std::sync::atomic::Ordering::SeqCst),
                cores: None,
                limit_unreadable: false,
            }),
        );
        let v = verdict_cache();
        let small = valid();
        let h = [5u8; 32];
        i.accept_for_tests(h, 0);
        // Usable share is less than 2 GiB; leave under the ceiling free.
        rss.store(2 * GIB - 300 * MIB, std::sync::atomic::Ordering::SeqCst);
        let before = count(&m, Outcome::Busy);
        let e = compile_err(i.admit(&h, &small, CodeKind::Contract, &v));
        assert!(matches!(e, WasmError::Compile(_)), "{e:?}");
        assert_eq!(count(&m, Outcome::Busy), before + 1);
        assert_eq!(i.budget().reserved(), 0);
    }

    /// Repeated requests for refused bytes are counted, under the reason.
    #[test]
    fn refusals_answered_from_the_verdict_cache_are_counted() {
        let (i, m) = iso(CompileIsolationMode::Enforce, false, Some("alloc"));
        let v = verdict_cache();
        let code = valid();
        for _ in 0..4 {
            assert!(matches!(
                i.admit(&hash(&code), &code, CodeKind::Contract, &v),
                Err(WasmError::CompileRefused(CompileRefusal::OverMemory))
            ));
        }
        assert_eq!(count(&m, Outcome::OverMemory), 1, "one worker verdict");
        assert_eq!(count(&m, Outcome::CachedOverMemory), 3, "three repeats");
    }

    /// Where the budget cannot cover a large accepted module's compile it is not
    /// compiled beside the node: a transient refusal, as for a worker.
    #[test]
    fn a_large_accepted_module_is_refused_transiently_when_memory_is_gone() {
        let rss = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let rss2 = rss.clone();
        let (i, _) = iso_on(
            CompileIsolationMode::Enforce,
            false,
            Launcher::for_tests(None),
            small_policy(),
            Arc::new(move || HostLimits {
                memory_limit: 2 * GIB,
                node_rss: rss2.load(std::sync::atomic::Ordering::SeqCst),
                cores: None,
                limit_unreadable: false,
            }),
        );
        let v = verdict_cache();
        let big = large_valid();
        i.admit(&hash(&big), &big, CodeKind::Contract, &v)
            .unwrap()
            .finish(true);
        // The node grows until nothing is left for a compile.
        rss.store(2 * GIB, std::sync::atomic::Ordering::SeqCst);
        let e = compile_err(i.admit(&hash(&big), &big, CodeKind::Contract, &v));
        assert!(matches!(e, WasmError::Compile(_)), "{e:?}");
    }

    /// The worker's parent-death signal follows the thread that started it, so
    /// every worker is started from one thread that lives as long as the node.
    #[test]
    fn a_worker_outlives_the_thread_that_asked_for_it() {
        let l = Launcher::for_tests(Some("sleep"));
        let mut child = std::thread::spawn(move || {
            super::super::supervisor::spawn_child(l.command()).expect("spawned")
        })
        .join()
        .expect("the requesting thread ended");
        std::thread::sleep(Duration::from_millis(1_500));
        assert!(
            child.try_wait().expect("wait").is_none(),
            "the worker died with the thread that requested it"
        );
        let pid = child.id() as libc::pid_t;
        // It leads a process group of its own, which is what the supervisor kills.
        // SAFETY: plain syscalls on our own child.
        assert_eq!(unsafe { libc::getpgid(pid) }, pid);
        // SAFETY: as above.
        unsafe { libc::kill(-pid, libc::SIGKILL) };
        drop(child.wait());
    }

    #[test]
    fn the_engine_path_checks_contract_code_and_keeps_delegate_code_unchanged() {
        use crate::wasm_runtime::RuntimeConfig;
        let cfg = |hosted| RuntimeConfig {
            compile_isolation: CompileIsolationMode::Enforce,
            compile_isolation_delegates: hosted,
            compile_worker_test: Some((Launcher::for_tests(Some("alloc-big")), small_policy())),
            ..RuntimeConfig::default()
        };
        let code = valid();
        // Every worker runs out of memory, so anything checked is refused.
        let mut e = WasmtimeEngine::new(&cfg(false), false).unwrap();
        let r = e.compile_contract(&code);
        assert!(
            matches!(
                r,
                Err(WasmError::CompileRefused(CompileRefusal::OverMemory))
            ),
            "{:?}",
            r.err()
        );
        assert!(
            e.compile(&code).is_ok(),
            "non-hosted delegate code is not checked"
        );
        let mut e = WasmtimeEngine::new(&cfg(true), false).unwrap();
        assert!(matches!(
            e.compile(&code),
            Err(WasmError::CompileRefused(CompileRefusal::OverMemory))
        ));
    }

    // -----------------------------------------------------------------------
    // Real compiles under tight caps: nothing here depends on how a module is
    // shaped beyond its size.
    // -----------------------------------------------------------------------

    /// A large ordinary module: straight-line integer arithmetic in many
    /// functions, `ops` operations in each.
    fn large_arithmetic_module(functions: usize, ops: usize) -> Vec<u8> {
        let mut wat = String::from("(module\n");
        for f in 0..functions {
            wat.push_str("(func (param i32) (result i32) local.get 0\n");
            for k in 0..ops {
                let c = (f * 31 + k * 7) as i32 & 0xffff;
                match k % 4 {
                    0 => wat.push_str(&format!("i32.const {c} i32.add\n")),
                    1 => wat.push_str(&format!("i32.const {c} i32.xor\n")),
                    2 => wat.push_str(&format!("i32.const {c} i32.mul\n")),
                    _ => wat.push_str(&format!("i32.const {c} i32.sub\n")),
                }
            }
            wat.push_str(")\n");
        }
        wat.push(')');
        wat::parse_str(&wat).expect("valid text")
    }

    /// A module of a few MiB: under a CPU cap far below what the real artifacts
    /// need, the real compile in the worker ends as `OverCpu` within the wall
    /// bound.
    #[test]
    fn a_large_ordinary_module_under_a_tight_cpu_cap_is_over_cpu() {
        let code = large_arithmetic_module(150, 8_000);
        assert!(code.len() > 3 * 1024 * 1024, "{}", code.len());
        let scan = crate::wasm_runtime::engine::compile_limits::check_module(&code);
        assert!(
            scan.is_ok(),
            "an ordinary module passes the scan: {:?}",
            scan.err()
        );
        let started = Instant::now();
        let (r, v) = run(None, caps(1_000, GIB, 60_000), &code);
        assert_eq!(v, Verdict::Refused(CompileRefusal::OverCpu), "{r:?}");
        assert!(
            started.elapsed() < Duration::from_secs(40),
            "{:?}",
            started.elapsed()
        );
        assert_parent_healthy();
    }

    /// The bundled River chat delegate under a data cap far below its need: the
    /// real compile runs out of memory and is classified as such, not as a crash.
    #[test]
    fn the_bundled_chat_delegate_under_a_tight_data_cap_is_over_memory_not_a_crash() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/data/compile_worker/chat_delegate.wasm");
        let code = std::fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let (r, v) = run(None, caps(60_000, 64 * MIB, 60_000), &code);
        assert_eq!(v, Verdict::Refused(CompileRefusal::OverMemory), "{r:?}");
        assert_parent_healthy();
    }
}
