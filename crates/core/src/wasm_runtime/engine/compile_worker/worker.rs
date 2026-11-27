//! The worker process: reads one module, judges it under hard limits and answers
//! with a verdict.
//!
//! It is the node binary run with a hidden argument, so its wasmtime and its
//! engine configuration are the node's by construction. It runs the same scan and
//! the same `Module::new` (on the same 4-thread pool) as the node, then drops the
//! module and reports. Nothing it compiles is returned.

use std::io::{Read, Write};

use super::protocol::{
    self, Caps, FLAG_CONSUME_FUEL, Identity, PROTOCOL_VERSION, Response, Status,
};
use crate::wasm_runtime::engine::compile_limits::{self, CompileRefusal};
use crate::wasm_runtime::engine::wasmtime_engine::{
    WasmtimeEngine, WorkerCompile, WorkerEngine, compile_and_classify, engine_compat_id,
};

/// First argument of the node binary that selects worker mode.
pub const WORKER_ARG: &str = "__compile-worker";
/// Argument naming the node's pid, so the worker can notice it was orphaned.
pub const PARENT_PID_ARG: &str = "--parent-pid";

/// Identity of this build, compared with the node's during the handshake.
pub(crate) fn build_id() -> [u8; 16] {
    let text = format!(
        "{}|{}",
        env!("CARGO_PKG_VERSION"),
        option_env!("GIT_COMMIT_HASH").unwrap_or("")
    );
    let mut id = [0u8; 16];
    id.copy_from_slice(&blake3::hash(text.as_bytes()).as_bytes()[..16]);
    id
}

/// The outcome of judging one module in the current process.
pub(crate) struct JobOutcome {
    pub status: Status,
    pub reason: String,
}

fn outcome(status: Status, reason: impl Into<String>) -> JobOutcome {
    JobOutcome {
        status,
        reason: reason.into(),
    }
}

/// The worker body: the node's scan, then the node's compile, on `engine`.
/// Runs under whatever limits the process has; it applies none itself.
pub(crate) fn judge_module(engine: &WorkerEngine, code: &[u8]) -> JobOutcome {
    if let Err(refusal) = compile_limits::check_input_size(code) {
        return outcome(Status::RefusedScan, refusal.to_string());
    }
    if let Err(refusal) = compile_limits::check_module(code) {
        return outcome(Status::RefusedScan, refusal.to_string());
    }
    match compile_and_classify(engine, code) {
        WorkerCompile::Compiled => outcome(Status::Ok, ""),
        WorkerCompile::OutOfMemory(text) => outcome(Status::OutOfMemory, text),
        WorkerCompile::Invalid(text) => {
            outcome(Status::Invalid, CompileRefusal::Malformed(text).to_string())
        }
        WorkerCompile::Internal(text) => outcome(Status::InternalError, text),
    }
}

// ---------------------------------------------------------------------------
// Process-level behaviour (Linux).
// ---------------------------------------------------------------------------

#[cfg(target_os = "linux")]
mod sys {
    use std::io::Read;

    /// A protection or limit the worker could not install.
    #[derive(Debug)]
    pub(super) struct InstallFailed(pub String);

    /// `prctl` is variadic: every argument must be passed at the width the
    /// kernel reads (an unsigned long), which a bare integer literal is not.
    const ZERO: libc::c_ulong = 0;

    fn os_error(what: &str) -> InstallFailed {
        InstallFailed(format!("{what}: {}", std::io::Error::last_os_error()))
    }

    /// Die with the parent, and notice a parent that was already gone.
    ///
    /// `PDEATHSIG` fires only for a parent THREAD that dies after the call (the
    /// node starts every worker from one thread that lives as long as the
    /// process), so the pid check after it closes the window between spawn and
    /// this call.
    pub(super) fn tie_to_parent(expected_parent: i32) -> Result<(), InstallFailed> {
        // SAFETY: plain prctl/getppid calls with constant arguments.
        unsafe {
            if libc::prctl(
                libc::PR_SET_PDEATHSIG,
                libc::SIGKILL as libc::c_ulong,
                ZERO,
                ZERO,
                ZERO,
            ) != 0
            {
                return Err(os_error("PR_SET_PDEATHSIG"));
            }
            if libc::getppid() != expected_parent {
                libc::_exit(3);
            }
        }
        Ok(())
    }

    /// Close every descriptor above stderr.
    pub(super) fn close_inherited_fds() {
        // SAFETY: close_range takes plain integers; a failure (old kernel) falls
        // through to the /proc walk.
        let r = unsafe {
            libc::syscall(
                libc::SYS_close_range,
                3 as libc::c_ulong,
                libc::c_uint::MAX as libc::c_ulong,
                ZERO,
            )
        };
        if r == 0 {
            return;
        }
        let Ok(dir) = std::fs::read_dir("/proc/self/fd") else {
            return;
        };
        let fds: Vec<libc::c_int> = dir
            .filter_map(|e| e.ok()?.file_name().to_str()?.parse().ok())
            .filter(|fd| *fd >= 3)
            .collect();
        for fd in fds {
            // SAFETY: closing a descriptor number we listed; the directory
            // handle's own fd is among them and closing it again is harmless.
            unsafe { libc::close(fd) };
        }
    }

    pub(super) fn prefer_me_for_oom_kill() -> Result<(), InstallFailed> {
        std::fs::write("/proc/self/oom_score_adj", b"1000")
            .map_err(|e| InstallFailed(format!("oom_score_adj: {e}")))
    }

    /// Own-process hardening. `nice` is a courtesy, not a protection: its return
    /// value is ambiguous (-1 is also a valid priority) and nothing depends on it.
    pub(super) fn harden() -> Result<(), InstallFailed> {
        // SAFETY: constant-argument prctl calls.
        unsafe {
            if libc::prctl(libc::PR_SET_DUMPABLE, ZERO, ZERO, ZERO, ZERO) != 0 {
                return Err(os_error("PR_SET_DUMPABLE"));
            }
            if libc::prctl(
                libc::PR_SET_NO_NEW_PRIVS,
                1 as libc::c_ulong,
                ZERO,
                ZERO,
                ZERO,
            ) != 0
            {
                return Err(os_error("PR_SET_NO_NEW_PRIVS"));
            }
            libc::nice(10);
        }
        Ok(())
    }

    /// The type `setrlimit` takes for the resource: glibc names its own, musl
    /// uses a plain `int`. The shipped Linux binaries are musl.
    #[cfg(target_env = "musl")]
    type Resource = libc::c_int;
    #[cfg(not(target_env = "musl"))]
    type Resource = libc::__rlimit_resource_t;

    fn set(resource: Resource, name: &str, soft: u64, hard: u64) -> Result<(), InstallFailed> {
        let lim = libc::rlimit {
            rlim_cur: soft as libc::rlim_t,
            rlim_max: hard as libc::rlim_t,
        };
        // SAFETY: `lim` is a valid, initialised rlimit.
        if unsafe { libc::setrlimit(resource, &lim) } != 0 {
            return Err(os_error(&format!("setrlimit {name}")));
        }
        Ok(())
    }

    /// Current virtual size of this process in bytes (mapped binary included).
    fn vm_size_bytes() -> u64 {
        status_kib("VmSize:").saturating_mul(1024)
    }

    pub(super) fn status_kib(key: &str) -> u64 {
        let mut text = String::new();
        if let Ok(mut f) = std::fs::File::open("/proc/self/status") {
            drop(f.read_to_string(&mut text));
        }
        text.lines()
            .find_map(|l| l.strip_prefix(key))
            .and_then(|rest| rest.split_whitespace().next()?.parse().ok())
            .unwrap_or(0)
    }

    /// Apply the caps. CPU is whole seconds rounded up, hard limit one second
    /// above the soft one, so the enforced limit is never below the stated cap.
    /// `RLIMIT_DATA` is the memory limit; `RLIMIT_AS` is a second, looser one
    /// that leaves room for the mapped binary and reserved address space.
    ///
    /// Every call is checked: a hard limit inherited from the node below the one
    /// asked for makes `setrlimit` fail, and a worker running under an unknown
    /// limit would pass its death off as the module's.
    pub(super) fn apply_caps(cpu_ms: u64, data_bytes: u64) -> Result<(), InstallFailed> {
        let soft = cpu_ms.div_ceil(1000).max(1);
        set(libc::RLIMIT_CORE, "CORE", 0, 0)?;
        set(libc::RLIMIT_FSIZE, "FSIZE", 0, 0)?;
        set(libc::RLIMIT_CPU, "CPU", soft, soft + 1)?;
        let as_limit = vm_size_bytes().saturating_add(data_bytes.saturating_mul(4));
        set(libc::RLIMIT_AS, "AS", as_limit, as_limit)?;
        set(libc::RLIMIT_DATA, "DATA", data_bytes, data_bytes)?;
        Ok(())
    }

    /// CPU time of this process, all threads, in milliseconds.
    pub(super) fn cpu_ms() -> u64 {
        // SAFETY: getrusage fills a zeroed struct we own.
        let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
        // SAFETY: getrusage writes only into the struct above.
        let r = unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
        if r != 0 {
            return 0;
        }
        let ms = |t: libc::timeval| (t.tv_sec as u64) * 1000 + (t.tv_usec as u64) / 1000;
        ms(ru.ru_utime) + ms(ru.ru_stime)
    }
}

/// Run the worker. Never returns: it exits 0 after writing one response, or
/// dies (a signal or a nonzero exit) and the parent classifies the death.
#[cfg(target_os = "linux")]
pub(crate) fn run_worker(expected_parent: i32) -> ! {
    let early = sys::tie_to_parent(expected_parent)
        .and_then(|()| {
            sys::close_inherited_fds();
            sys::prefer_me_for_oom_kill()
        })
        .and_then(|()| sys::harden());

    let mut stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let header = match protocol::read_request_header(&mut stdin) {
        Ok(h) => h,
        Err(_) => std::process::exit(4),
    };
    let reply = |status: Status, reason: &str| -> ! {
        let frame = protocol::encode_response(&Response {
            status,
            vm_hwm_kib: sys::status_kib("VmHWM:"),
            cpu_ms: sys::cpu_ms(),
            reason: reason.to_string(),
        });
        drop(protocol::write_all(&mut std::io::stdout().lock(), &frame));
        std::process::exit(0)
    };
    // Not a verdict on the module: say so, and the node counts it as an
    // infrastructure failure.
    if let Err(e) = early.and_then(|()| sys::apply_caps(header.caps.cpu_ms, header.caps.data_bytes))
    {
        reply(Status::LimitInstallFailed, &e.0);
    }

    if header.version != PROTOCOL_VERSION {
        reply(Status::VersionMismatch, "protocol version");
    }
    let code = match protocol::read_wasm(&mut stdin, header.wasm_len) {
        Ok(c) => c,
        Err(_) => std::process::exit(5),
    };
    drop(stdout.flush());
    drop(stdout);

    #[cfg(test)]
    if let Some(fault) = fault::requested(&code) {
        fault::run(fault, &header.caps, &code);
    }

    let engine = match WasmtimeEngine::create_worker_engine(
        header.identity.flags & FLAG_CONSUME_FUEL != 0,
    ) {
        Ok(e) => e,
        Err(e) => reply(Status::InternalError, &format!("engine: {e}")),
    };
    let ours = Identity {
        engine_compat_id: engine_compat_id(&engine),
        build_id: build_id(),
        flags: header.identity.flags,
    };
    if ours != header.identity {
        reply(Status::VersionMismatch, "engine or build identity");
    }
    let out = judge_module(&engine, &code);
    reply(out.status, &out.reason)
}

/// Entry point for the node binary: `freenet __compile-worker --parent-pid N`.
#[cfg(target_os = "linux")]
pub(crate) fn worker_main_from_args() -> ! {
    let args: Vec<String> = std::env::args().collect();
    let parent = args
        .iter()
        .position(|a| a == PARENT_PID_ARG)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse::<i32>().ok());
    match parent {
        Some(pid) => run_worker(pid),
        None => std::process::exit(2),
    }
}

/// No worker exists off Linux: isolation is off there, so nothing starts one.
#[cfg(not(target_os = "linux"))]
pub(crate) fn worker_main_from_args() -> ! {
    std::process::exit(2)
}

#[allow(dead_code)]
fn _assert_unused(_: &mut dyn Read, _: &mut dyn Write, _: &Caps) {}

/// Fault modes, present only in test builds (the test binary is the worker for
/// the process-level tests). They are selected by an environment variable the
/// parent's test launcher sets.
#[cfg(all(test, target_os = "linux"))]
pub(crate) mod fault {
    use super::Caps;
    use std::io::Write;

    pub(crate) const ENV: &str = "FNCW_TEST_FAULT";

    pub(crate) fn requested(code: &[u8]) -> Option<String> {
        let mode = std::env::var(ENV).ok()?;
        // A `-big` mode leaves the 8-byte startup probe module alone.
        match mode.strip_suffix("-big") {
            Some(_) if code.len() <= 8 => None,
            Some(name) => Some(name.to_string()),
            None => Some(mode),
        }
    }

    fn write_reply(status: super::Status, reason: &str) -> ! {
        let frame = super::protocol::encode_response(&super::Response {
            status,
            vm_hwm_kib: super::sys::status_kib("VmHWM:"),
            cpu_ms: super::sys::cpu_ms(),
            reason: reason.to_string(),
        });
        drop(super::protocol::write_all(
            &mut std::io::stdout().lock(),
            &frame,
        ));
        std::process::exit(0)
    }

    // Infinite recursion on purpose: the stack-overflow fault.
    #[allow(unconditional_recursion)]
    fn recurse(depth: u64) -> u64 {
        let pad = [depth as u8; 4096];
        std::hint::black_box(&pad);
        recurse(depth + 1) + u64::from(pad[0])
    }

    pub(crate) fn run(mode: String, _caps: &Caps, code: &[u8]) -> ! {
        match mode.as_str() {
            // Allocate and touch private memory until the limit stops us.
            "alloc" => {
                let mut held: Vec<Vec<u8>> = Vec::new();
                loop {
                    let mut chunk = vec![0u8; 8 << 20];
                    for i in (0..chunk.len()).step_by(4096) {
                        chunk[i] = 1;
                    }
                    held.push(chunk);
                }
            }
            // Four threads fault in memory as fast as they can.
            "burst" => {
                let hs: Vec<_> = (0..4)
                    .map(|_| {
                        std::thread::spawn(|| {
                            let mut held: Vec<Vec<u8>> = Vec::new();
                            loop {
                                let mut chunk = vec![0u8; 1 << 20];
                                for i in (0..chunk.len()).step_by(4096) {
                                    chunk[i] = 1;
                                }
                                held.push(chunk);
                            }
                        })
                    })
                    .collect();
                for h in hs {
                    drop(h.join());
                }
                std::process::exit(0)
            }
            "spin" => {
                let hs: Vec<_> = (0..4)
                    .map(|_| {
                        std::thread::spawn(|| {
                            loop {
                                std::hint::spin_loop()
                            }
                        })
                    })
                    .collect();
                for h in hs {
                    drop(h.join());
                }
                std::process::exit(0)
            }
            "spin1" => loop {
                std::hint::spin_loop()
            },
            "sleep" => loop {
                std::thread::sleep(std::time::Duration::from_secs(60))
            },
            "abort" => std::process::abort(),
            "segv" => {
                // SAFETY: deliberate fault.
                unsafe { libc::raise(libc::SIGSEGV) };
                std::process::exit(9)
            }
            "stack" => std::process::exit(recurse(0) as i32),
            "panic" => panic!("fault injection"),
            "exit-silent" => std::process::exit(0),
            "exit-7" => std::process::exit(7),
            "stderr-flood" => {
                let mut err = std::io::stderr();
                let chunk = vec![b'x'; 1 << 16];
                for _ in 0..1600 {
                    drop(err.write_all(&chunk));
                }
                write_reply(super::Status::Ok, "")
            }
            "garbage" => {
                let mut out = std::io::stdout().lock();
                drop(out.write_all(&[0xAB; 4096]));
                drop(out.flush());
                std::process::exit(0)
            }
            "huge-reply" => {
                let mut out = std::io::stdout().lock();
                drop(
                    out.write_all(&super::protocol::encode_response(&super::Response {
                        status: super::Status::Ok,
                        vm_hwm_kib: 0,
                        cpu_ms: 0,
                        reason: String::new(),
                    })),
                );
                let chunk = vec![0u8; 1 << 16];
                for _ in 0..1600 {
                    if out.write_all(&chunk).is_err() {
                        break;
                    }
                }
                std::process::exit(0)
            }
            "report-env" => {
                let n = std::env::vars().filter(|(k, _)| k != ENV).count();
                write_reply(super::Status::InternalError, &format!("env={n}"))
            }
            "report-fds" => {
                let fds: Vec<String> = std::fs::read_dir("/proc/self/fd")
                    .map(|d| {
                        d.filter_map(|e| e.ok()?.file_name().into_string().ok())
                            .collect()
                    })
                    .unwrap_or_default();
                let mut fds: Vec<u32> = fds.iter().filter_map(|f| f.parse().ok()).collect();
                fds.sort_unstable();
                write_reply(super::Status::InternalError, &format!("fds={fds:?}"))
            }
            "report-len" => {
                write_reply(super::Status::InternalError, &format!("len={}", code.len()))
            }
            other => write_reply(
                super::Status::InternalError,
                &format!("unknown fault {other}"),
            ),
        }
    }
}
