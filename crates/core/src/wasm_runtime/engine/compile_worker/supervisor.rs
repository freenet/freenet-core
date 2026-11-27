//! The node's side of the worker: start it, give it one module, stop it at the
//! wall limit, reap it, and turn how it ended into a verdict.

use std::ffi::OsString;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Arc, Mutex, OnceLock};
// The system clock is deliberate here, not `TimeSource`: these instants time a
// real child process (its wall limit and exit grace), which no simulated clock
// can advance. The breaker and back-off deadlines use the node's `TimeSource`
// (see `isolation.rs`).
use std::time::{Duration, Instant};

use super::protocol::{self, Caps, Identity, Response, Status};
use crate::wasm_runtime::engine::compile_limits::CompileRefusal;

/// Most of the worker's stderr kept for classification and logs.
const STDERR_TAIL_BYTES: usize = 4096;
/// How long a worker may take to exit after it has sent its answer.
const EXIT_GRACE: Duration = Duration::from_secs(5);
/// Leading bytes tolerated before the response magic (test launchers only: the
/// test harness prints a line to stdout before the test body runs).
const MAX_PREFIX_BYTES: usize = 4096;

/// How to start a worker.
#[derive(Debug, Clone)]
pub(crate) struct Launcher {
    exe: PathBuf,
    args: Vec<OsString>,
    env: Vec<(OsString, OsString)>,
    /// Skip bytes before the response magic. Never set in production.
    tolerate_stdout_prefix: bool,
    /// Lower the child's `RLIMIT_CPU` hard limit before it starts, so the worker
    /// inherits a limit below the one it will ask for (tests only).
    #[cfg(all(test, target_os = "linux"))]
    inherit_cpu_hard_secs: Option<u64>,
}

impl Launcher {
    /// The node's own binary, in worker mode. On Linux this is `/proc/self/exe`,
    /// which names the binary this process is running even after an update has
    /// replaced the file on disk, so the worker is always the same build.
    #[cfg(target_os = "linux")]
    pub(crate) fn production() -> Option<Self> {
        let exe = if std::path::Path::new("/proc/self/exe").exists() {
            PathBuf::from("/proc/self/exe")
        } else {
            std::env::current_exe().ok()?
        };
        Some(Self::production_at(exe))
    }

    /// The production argument vector for the node binary at `exe`.
    #[cfg(target_os = "linux")]
    pub(crate) fn production_at(exe: PathBuf) -> Self {
        Self {
            exe,
            args: vec![
                super::worker::WORKER_ARG.into(),
                super::worker::PARENT_PID_ARG.into(),
                std::process::id().to_string().into(),
            ],
            env: Vec::new(),
            tolerate_stdout_prefix: false,
            #[cfg(all(test, target_os = "linux"))]
            inherit_cpu_hard_secs: None,
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub(crate) fn production() -> Option<Self> {
        None
    }

    /// The command that starts a worker: empty environment, working directory
    /// `/`, all three standard streams set explicitly, and a process group of its
    /// own (so the supervisor can kill the worker and anything it started).
    pub(crate) fn command(&self) -> std::process::Command {
        use std::os::unix::process::CommandExt;
        use std::process::{Command, Stdio};
        let mut cmd = Command::new(&self.exe);
        cmd.args(&self.args)
            .env_clear()
            .envs(self.env.iter().map(|(k, v)| (k, v)))
            .current_dir("/")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0);
        #[cfg(all(test, target_os = "linux"))]
        if let Some(hard) = self.inherit_cpu_hard_secs {
            // SAFETY: only async-signal-safe calls (setrlimit) between fork and exec.
            unsafe {
                cmd.pre_exec(move || {
                    let lim = libc::rlimit {
                        rlim_cur: hard as libc::rlim_t,
                        rlim_max: hard as libc::rlim_t,
                    };
                    if libc::setrlimit(libc::RLIMIT_CPU, &lim) != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
        }
        cmd
    }

    /// The test binary playing the worker (see `tests::worker_entry`), with an
    /// optional fault mode.
    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn for_tests(fault: Option<&str>) -> Self {
        let mut env = vec![(
            OsString::from(super::tests::PARENT_ENV),
            OsString::from(std::process::id().to_string()),
        )];
        if let Some(f) = fault {
            env.push((OsString::from(super::worker::fault::ENV), OsString::from(f)));
        }
        Self {
            exe: std::env::current_exe().expect("test binary path"),
            args: [
                "--exact",
                super::tests::WORKER_ENTRY_TEST,
                "--nocapture",
                "--test-threads=1",
            ]
            .iter()
            .map(OsString::from)
            .collect(),
            env,
            tolerate_stdout_prefix: true,
            inherit_cpu_hard_secs: None,
        }
    }

    /// Workers that inherit an `RLIMIT_CPU` hard limit of `secs` seconds.
    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn with_inherited_cpu_hard_limit(mut self, secs: u64) -> Self {
        self.inherit_cpu_hard_secs = Some(secs);
        self
    }

    /// A launcher whose program does not exist.
    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn missing_for_tests() -> Self {
        Self {
            exe: PathBuf::from("/nonexistent/freenet-compile-worker"),
            args: Vec::new(),
            env: Vec::new(),
            tolerate_stdout_prefix: false,
            inherit_cpu_hard_secs: None,
        }
    }

    /// Same launcher with another parent pid in its environment (tests of the
    /// orphan check).
    #[cfg(all(test, target_os = "linux"))]
    pub(crate) fn with_parent_pid(mut self, pid: i32) -> Self {
        for (k, v) in &mut self.env {
            if k == super::tests::PARENT_ENV {
                *v = pid.to_string().into();
            }
        }
        self
    }
}

/// How the worker process ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum End {
    Exited(i32),
    Signaled(i32),
}

/// Everything observed about one worker run.
#[derive(Debug)]
pub(crate) struct RunReport {
    pub end: End,
    pub response: Option<Response>,
    /// Why no response was read, when none was.
    pub protocol_error: Option<String>,
    pub killed_by_us: bool,
    pub wall: Duration,
    /// CPU time of the worker's process as the kernel counted it (`wait4`).
    pub kernel_cpu_ms: u64,
    pub stderr_tail: String,
}

/// Why a worker could not be run at all.
#[derive(Debug)]
pub(crate) struct SpawnFailed(pub String);

struct Tail {
    buf: Vec<u8>,
}

impl Tail {
    fn push(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
        if self.buf.len() > STDERR_TAIL_BYTES {
            let cut = self.buf.len() - STDERR_TAIL_BYTES;
            self.buf.drain(..cut);
        }
    }
}

struct SpawnRequest {
    cmd: std::process::Command,
    reply: mpsc::Sender<std::io::Result<std::process::Child>>,
}

/// Every worker is started from this one thread, which lives as long as the
/// process. This thread is deliberately not registered with a task monitor:
/// nothing restarts it, and nothing observes it dying. If it ever dies, the next
/// `spawn_child` call returns `SpawnFailed("the spawner thread is gone")`; the
/// caller counts a spawn failure and opens the global breaker (1 s doubling to
/// 60 s), so modules not yet accepted are refused with a transient error while
/// enforcing (and skipped in dry-run), and no code is accepted unchecked. Modules
/// already accepted keep compiling in the node's process. The failure is visible
/// in the spawn-failure counter and in an error log on every attempt. The worker's parent-death signal (`PR_SET_PDEATHSIG`) fires when the
/// THREAD that started it exits, not the process, so a worker started from a
/// compile thread (a blocking-pool thread that the runtime retires) would be
/// killed mid-module the moment that thread went away.
fn spawner() -> Option<&'static mpsc::Sender<SpawnRequest>> {
    static SPAWNER: OnceLock<Option<mpsc::Sender<SpawnRequest>>> = OnceLock::new();
    SPAWNER
        .get_or_init(|| {
            let (tx, rx) = mpsc::channel::<SpawnRequest>();
            std::thread::Builder::new()
                .name("compile-worker-spawner".into())
                .spawn(move || {
                    for mut req in rx {
                        let child = req.cmd.spawn();
                        drop(req.reply.send(child));
                    }
                })
                .ok()
                .map(|_| tx)
        })
        .as_ref()
}

pub(crate) fn spawn_child(cmd: std::process::Command) -> Result<std::process::Child, SpawnFailed> {
    let tx = spawner().ok_or_else(|| SpawnFailed("could not start the spawner thread".into()))?;
    let (reply, rx) = mpsc::channel();
    tx.send(SpawnRequest { cmd, reply })
        .map_err(|_| SpawnFailed("the spawner thread is gone".into()))?;
    rx.recv()
        .map_err(|_| SpawnFailed("the spawner thread is gone".into()))?
        .map_err(|e| SpawnFailed(e.to_string()))
}

/// Kill the worker's whole process group (it leads one, see
/// [`Launcher::command`]); fall back to the pid alone.
fn kill_worker(pid: libc::pid_t) {
    // SAFETY: plain signal syscalls on our own un-reaped child's group and pid.
    unsafe {
        if libc::kill(-pid, libc::SIGKILL) != 0 {
            libc::kill(pid, libc::SIGKILL);
        }
    }
}

/// Kills and reaps the worker if the supervising code unwinds or returns early.
struct Reaper {
    pid: libc::pid_t,
    reaped: bool,
}

impl Drop for Reaper {
    fn drop(&mut self) {
        if !self.reaped {
            kill_worker(self.pid);
            // SAFETY: the pid is our own un-reaped child, so it cannot have been
            // reused; wait is a plain syscall.
            unsafe {
                let mut status = 0;
                libc::waitpid(self.pid, &mut status, 0);
            }
        }
    }
}

fn tv_ms(t: libc::timeval) -> u64 {
    (t.tv_sec as u64) * 1000 + (t.tv_usec as u64) / 1000
}

/// Run one worker to completion (or to its wall limit) on `code`.
pub(crate) fn run_worker_once(
    launcher: &Launcher,
    identity: &Identity,
    caps: &Caps,
    code: &[u8],
) -> Result<RunReport, SpawnFailed> {
    use std::os::unix::process::ExitStatusExt;

    let started = Instant::now();
    let mut child = spawn_child(launcher.command())?;
    let pid = child.id() as libc::pid_t;
    let mut reaper = Reaper { pid, reaped: false };

    let header = protocol::encode_request_header(identity, caps, code.len());
    // Infallible: `Launcher::command` sets all three stdio handles to
    // `Stdio::piped()` before the child is spawned, so each `take()` returns
    // `Some` the first (and only) time.
    let mut stdin = child.stdin.take().expect("piped");
    let mut stdout = child.stdout.take().expect("piped");
    let mut stderr = child.stderr.take().expect("piped");
    // The child handle is only used for its pipes from here: the process is
    // reaped with wait4 below so that its resource usage is available.
    let code_owned = code.to_vec();

    let writer = std::thread::spawn(move || {
        // A worker that dies early closes the pipe; that is not our error.
        drop(
            stdin
                .write_all(&header)
                .and_then(|()| stdin.write_all(&code_owned)),
        );
        drop(stdin.flush());
        drop(stdin);
    });
    let tolerate = launcher.tolerate_stdout_prefix;
    let (resp_tx, resp_rx) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        let res = read_response_tolerant(&mut stdout, tolerate);
        drop(resp_tx.send(res));
        // Drain whatever else the worker prints so it never blocks on a full pipe
        // before the wall limit; bounded by the wall limit and the grace period.
        let mut sink = [0u8; 8192];
        while matches!(stdout.read(&mut sink), Ok(n) if n > 0) {}
    });
    let tail = Arc::new(Mutex::new(Tail { buf: Vec::new() }));
    let tail_w = tail.clone();
    let err_reader = std::thread::spawn(move || {
        let mut chunk = [0u8; 8192];
        while let Ok(n) = stderr.read(&mut chunk) {
            if n == 0 {
                break;
            }
            tail_w
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(&chunk[..n]);
        }
    });

    let deadline = started + Duration::from_millis(caps.wall_ms);
    let mut killed_by_us = false;
    let mut response: Option<Result<Option<Response>, String>> = None;
    let mut answered_at: Option<Instant> = None;
    let mut sleep = Duration::from_millis(1);
    let (end, kernel_cpu_ms) = loop {
        if response.is_none() {
            if let Ok(r) = resp_rx.try_recv() {
                response = Some(r);
                answered_at = Some(Instant::now());
            }
        }
        let mut status = 0;
        // SAFETY: zeroed rusage is a valid out-parameter; pid is our child.
        let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
        // SAFETY: wait4 on our own child with valid out-pointers that outlive the call.
        let r = unsafe { libc::wait4(pid, &mut status, libc::WNOHANG, &mut ru) };
        if r == pid {
            reaper.reaped = true;
            let st = std::process::ExitStatus::from_raw(status);
            let end = match st.signal() {
                Some(sig) => End::Signaled(sig),
                None => End::Exited(st.code().unwrap_or(-1)),
            };
            break (end, tv_ms(ru.ru_utime) + tv_ms(ru.ru_stime));
        }
        if r < 0 {
            // Cannot happen for our own child; do not spin.
            reaper.reaped = true;
            break (End::Exited(-1), 0);
        }
        let now = Instant::now();
        let over_wall = now >= deadline;
        let over_grace = answered_at.is_some_and(|t| now >= t + EXIT_GRACE);
        if (over_wall || over_grace) && !killed_by_us {
            killed_by_us = true;
            kill_worker(pid);
        }
        std::thread::sleep(sleep);
        sleep = (sleep * 2).min(Duration::from_millis(20));
    };
    let wall = started.elapsed();
    drop(writer.join());
    drop(reader.join());
    drop(err_reader.join());
    if response.is_none() {
        response = resp_rx.try_recv().ok();
    }
    let stderr_tail = {
        let t = tail.lock().unwrap_or_else(|e| e.into_inner());
        String::from_utf8_lossy(&t.buf).into_owned()
    };
    let (response, protocol_error) = match response {
        Some(Ok(r)) => (r, None),
        Some(Err(e)) => (None, Some(e)),
        None => (None, None),
    };
    Ok(RunReport {
        end,
        response,
        protocol_error,
        killed_by_us,
        wall,
        kernel_cpu_ms,
        stderr_tail,
    })
}

/// `Ok(None)` means the worker closed its output without sending anything: it
/// ended without answering, which is a crash and not a protocol fault.
fn read_response_tolerant(
    r: &mut impl Read,
    tolerate_prefix: bool,
) -> Result<Option<Response>, String> {
    let mut first = [0u8; 1];
    if r.read(&mut first).map_err(|e| e.to_string())? == 0 {
        return Ok(None);
    }
    let mut head = std::io::Cursor::new(first);
    if tolerate_prefix {
        // Slide a 4-byte window until the magic appears.
        let mut win = [0u8; 4];
        let mut seen = 0usize;
        let mut byte = first;
        loop {
            seen += 1;
            win.rotate_left(1);
            win[3] = byte[0];
            if win == protocol::MAGIC {
                break;
            }
            if seen > MAX_PREFIX_BYTES {
                return Err("no response magic".into());
            }
            if r.read(&mut byte).map_err(|e| e.to_string())? == 0 {
                return Ok(None);
            }
        }
        let mut chain = std::io::Cursor::new(protocol::MAGIC)
            .chain(r.take(protocol::MAX_RESPONSE_BYTES as u64));
        return protocol::read_response(&mut chain)
            .map(Some)
            .map_err(|e| e.to_string());
    }
    let mut limited = (&mut head)
        .chain(r)
        .take(protocol::MAX_RESPONSE_BYTES as u64);
    protocol::read_response(&mut limited)
        .map(Some)
        .map_err(|e| e.to_string())
}

// ---------------------------------------------------------------------------
// Classification
// ---------------------------------------------------------------------------

/// Why the node could not get an answer from a worker, so the module is neither
/// accepted nor refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BusyReason {
    VersionMismatch,
    ProtocolError,
    InternalError,
    /// The worker could not install a limit it was asked to (for example an
    /// inherited hard limit below the requested one), so it judged nothing.
    LimitsNotInstalled,
}

/// What the worker's run means for the module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Within the caps. Carries the worker's own measurements.
    Accepted {
        vm_hwm_kib: u64,
        cpu_ms: u64,
    },
    /// A property of the bytes under these caps, attributable to the module from
    /// the worker's own report or from the kernel's own enforcement of a limit the
    /// worker installed: remembered.
    Refused(CompileRefusal),
    /// The worker ended abnormally without a cap signature (a signal other than
    /// the CPU limit, a panic, a silent exit). The cause may be the environment
    /// (a pids limit, a hardened unit, a library fault), so it is not remembered
    /// as a property of the module; the caller backs off per module instead.
    Crashed,
    /// The wall limit passed with CPU below its cap: a load symptom.
    Timeout,
    /// Killed from outside with no cap signature (for example another process's
    /// OOM kill): transient.
    Interrupted,
    Busy(BusyReason),
}

/// Whole seconds the worker's `RLIMIT_CPU` soft limit was set to.
fn cpu_soft_limit_ms(caps: &Caps) -> u64 {
    caps.cpu_ms.div_ceil(1000).max(1) * 1000
}

fn allocation_failure(stderr: &str) -> bool {
    stderr.contains("memory allocation of") && stderr.contains("failed")
}

/// Turn one run into a verdict (the table in the design, one row per arm).
pub(crate) fn classify(report: &RunReport, caps: &Caps) -> Verdict {
    if let Some(resp) = &report.response {
        if report.killed_by_us && !matches!(report.end, End::Exited(0)) {
            // It answered, then would not exit.
            return Verdict::Busy(BusyReason::ProtocolError);
        }
        return match resp.status {
            Status::Ok => Verdict::Accepted {
                vm_hwm_kib: resp.vm_hwm_kib,
                cpu_ms: resp.cpu_ms,
            },
            Status::Invalid | Status::RefusedScan => Verdict::Refused(CompileRefusal::Malformed(
                crate::wasm_runtime::engine::compile_limits::bounded_text(&resp.reason),
            )),
            Status::OutOfMemory => Verdict::Refused(CompileRefusal::OverMemory),
            Status::InternalError => Verdict::Busy(BusyReason::InternalError),
            Status::VersionMismatch => Verdict::Busy(BusyReason::VersionMismatch),
            Status::LimitInstallFailed => Verdict::Busy(BusyReason::LimitsNotInstalled),
        };
    }
    let near_cpu_cap = report.kernel_cpu_ms + 1000 >= cpu_soft_limit_ms(caps);
    if report.killed_by_us {
        return if near_cpu_cap {
            Verdict::Refused(CompileRefusal::OverCpu)
        } else {
            Verdict::Timeout
        };
    }
    match report.end {
        End::Signaled(s) if s == libc::SIGXCPU => Verdict::Refused(CompileRefusal::OverCpu),
        End::Signaled(s) if s == libc::SIGKILL => {
            if near_cpu_cap {
                Verdict::Refused(CompileRefusal::OverCpu)
            } else {
                Verdict::Interrupted
            }
        }
        End::Signaled(s) if s == libc::SIGABRT && allocation_failure(&report.stderr_tail) => {
            Verdict::Refused(CompileRefusal::OverMemory)
        }
        // The worker's own exits for a request it could not read, or an orphan:
        // not a property of the bytes.
        End::Exited(3..=5) => Verdict::Busy(BusyReason::ProtocolError),
        End::Exited(0) if report.protocol_error.is_some() => {
            Verdict::Busy(BusyReason::ProtocolError)
        }
        // Any other abnormal end, including a clean exit that sent no answer.
        End::Signaled(_) | End::Exited(_) => Verdict::Crashed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caps() -> Caps {
        Caps {
            cpu_ms: 20_000,
            data_bytes: 1 << 30,
            wall_ms: 30_000,
        }
    }
    fn report(end: End) -> RunReport {
        RunReport {
            end,
            response: None,
            protocol_error: None,
            killed_by_us: false,
            wall: Duration::from_secs(1),
            kernel_cpu_ms: 100,
            stderr_tail: String::new(),
        }
    }
    fn with(status: Status) -> RunReport {
        let mut r = report(End::Exited(0));
        r.response = Some(Response {
            status,
            vm_hwm_kib: 5,
            cpu_ms: 6,
            reason: "why".into(),
        });
        r
    }

    #[test]
    fn answers_map_to_verdicts() {
        assert_eq!(
            classify(&with(Status::Ok), &caps()),
            Verdict::Accepted {
                vm_hwm_kib: 5,
                cpu_ms: 6
            }
        );
        assert!(matches!(
            classify(&with(Status::Invalid), &caps()),
            Verdict::Refused(CompileRefusal::Malformed(_))
        ));
        assert!(matches!(
            classify(&with(Status::RefusedScan), &caps()),
            Verdict::Refused(CompileRefusal::Malformed(_))
        ));
        assert_eq!(
            classify(&with(Status::OutOfMemory), &caps()),
            Verdict::Refused(CompileRefusal::OverMemory)
        );
        assert_eq!(
            classify(&with(Status::InternalError), &caps()),
            Verdict::Busy(BusyReason::InternalError)
        );
        assert_eq!(
            classify(&with(Status::VersionMismatch), &caps()),
            Verdict::Busy(BusyReason::VersionMismatch)
        );
        // A limit that could not be installed is not a verdict on the module.
        assert_eq!(
            classify(&with(Status::LimitInstallFailed), &caps()),
            Verdict::Busy(BusyReason::LimitsNotInstalled)
        );
        // Answered, then had to be killed: not trusted.
        let mut r = with(Status::Ok);
        r.killed_by_us = true;
        r.end = End::Signaled(libc::SIGKILL);
        assert_eq!(
            classify(&r, &caps()),
            Verdict::Busy(BusyReason::ProtocolError)
        );
    }

    #[test]
    fn deaths_map_to_verdicts() {
        // CPU limit signals.
        assert_eq!(
            classify(&report(End::Signaled(libc::SIGXCPU)), &caps()),
            Verdict::Refused(CompileRefusal::OverCpu)
        );
        // SIGKILL with CPU at the rounded soft limit minus one second: CPU cap.
        let mut r = report(End::Signaled(libc::SIGKILL));
        r.kernel_cpu_ms = 19_000;
        assert_eq!(
            classify(&r, &caps()),
            Verdict::Refused(CompileRefusal::OverCpu)
        );
        // SIGKILL below that boundary: an outside kill.
        r.kernel_cpu_ms = 18_999;
        assert_eq!(classify(&r, &caps()), Verdict::Interrupted);
        // Our own timer, CPU low: a timeout; CPU at the cap: the CPU cap.
        let mut r = report(End::Signaled(libc::SIGKILL));
        r.killed_by_us = true;
        assert_eq!(classify(&r, &caps()), Verdict::Timeout);
        r.kernel_cpu_ms = 25_000;
        assert_eq!(
            classify(&r, &caps()),
            Verdict::Refused(CompileRefusal::OverCpu)
        );
        // Abort with and without the allocation-failure message.
        let mut r = report(End::Signaled(libc::SIGABRT));
        assert_eq!(classify(&r, &caps()), Verdict::Crashed);
        r.stderr_tail = "memory allocation of 4336 bytes failed\n".into();
        assert_eq!(
            classify(&r, &caps()),
            Verdict::Refused(CompileRefusal::OverMemory)
        );
        // Other abnormal ends are crashes.
        for end in [
            End::Signaled(libc::SIGSEGV),
            End::Signaled(libc::SIGBUS),
            End::Signaled(libc::SIGILL),
            End::Exited(101),
            End::Exited(7),
            End::Exited(0),
        ] {
            assert_eq!(
                classify(&report(end.clone()), &caps()),
                Verdict::Crashed,
                "{end:?}"
            );
        }
        // Orphaned worker, or one that could not read the request: not the bytes'.
        assert_eq!(
            classify(&report(End::Exited(3)), &caps()),
            Verdict::Busy(BusyReason::ProtocolError)
        );
    }

    #[test]
    fn cpu_soft_limit_rounds_up() {
        let mut c = caps();
        c.cpu_ms = 20_001;
        assert_eq!(cpu_soft_limit_ms(&c), 21_000);
        c.cpu_ms = 1;
        assert_eq!(cpu_soft_limit_ms(&c), 1_000);
    }
}
