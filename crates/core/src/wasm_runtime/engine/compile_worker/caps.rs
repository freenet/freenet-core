//! Sizing the worker's caps from the service's cgroup.
//!
//! The installed service unit limits the whole service (the node and its
//! workers) with `MemoryMax` and `CPUQuota`. A worker that pushed the cgroup to
//! its limit would take the node down with it, so a worker's memory cap is the
//! memory still free under the limit at the moment it starts: 90% of the limit,
//! less the node's current resident memory, less what the other workers already
//! reserved (see `budget`). When there is no limit, the machine's memory stands
//! in for it.

use std::path::Path;

/// CPU time one module may use in the worker, all threads summed. It is a
/// little over 2.5 times the most any contract or delegate artifact in use
/// needed, measured in a debug-profile test binary on a loaded host.
pub(crate) const DEFAULT_CPU_MS: u64 = 30_000;
/// Wall time one module may take in the worker, at one core or more. 60 s is
/// also the node's operation time limit, so a longer wait could not help a
/// request anyway.
pub(crate) const DEFAULT_WALL_MS: u64 = 60_000;
/// The least `RLIMIT_DATA` a worker is given: 1.5 times the most private
/// writable memory any contract or delegate artifact in use needed to compile.
/// A start with less headroom than this is not attempted, because an artifact
/// that nodes carry could then be refused for want of memory the node could
/// have spared.
pub(crate) const DEFAULT_DATA_FLOOR_BYTES: u64 = 624 << 20;
/// The most memory one worker is ever given.
pub(crate) const DEFAULT_DATA_CEILING_BYTES: u64 = 1 << 30;
/// Most workers that run at once.
pub(crate) const MAX_GATE: usize = 2;

/// Limits read from the cgroup tree, each the smallest along the path to the
/// root. `None` means no limit was found.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub(crate) struct CgroupLimits {
    pub memory_max: Option<u64>,
    pub cpu_cores: Option<f64>,
    /// The process is in a non-root cgroup but its memory limit could not be
    /// read (the cgroup directory or mount is missing, or the limit file does
    /// not parse). This differs from `memory_max: None`, which means the limit
    /// was read and there is none.
    pub memory_unreadable: bool,
}

/// The cgroup v1 "no limit" value: `i64::MAX` rounded down to a page.
const V1_UNLIMITED_FROM: u64 = (i64::MAX as u64) & !4095;

/// `memory.max`: a byte count, or `max`.
pub(crate) fn parse_memory_max(text: &str) -> Option<u64> {
    text.trim().parse::<u64>().ok()
}

/// `cpu.max`: `<quota> <period>` in microseconds, or `max <period>`.
pub(crate) fn parse_cpu_max(text: &str) -> Option<f64> {
    let mut it = text.split_whitespace();
    let quota: f64 = it.next()?.parse().ok()?;
    let period: f64 = it.next()?.parse().ok()?;
    (quota > 0.0 && period > 0.0).then(|| quota / period)
}

/// The cgroup v2 path in a `/proc/<pid>/cgroup` text (`0::<path>`).
pub(crate) fn cgroup_path(proc_cgroup: &str) -> Option<&str> {
    proc_cgroup
        .lines()
        .find_map(|l| l.strip_prefix("0::"))
        .map(str::trim)
}

/// Read the cgroup v2 limits of `path` under `root` (`/sys/fs/cgroup`), taking
/// the smallest value along the path to the root.
fn read_v2(root: &Path, path: &str) -> CgroupLimits {
    let mut out = CgroupLimits::default();
    let mut dir = root.join(path.trim_start_matches('/'));
    if !path.trim_matches('/').is_empty() && !dir.is_dir() {
        out.memory_unreadable = true;
    }
    loop {
        match std::fs::read_to_string(dir.join("memory.max")) {
            Ok(text) => match parse_memory_max(&text) {
                Some(m) => out.memory_max = Some(out.memory_max.map_or(m, |c| c.min(m))),
                None if text.trim() == "max" => {}
                None => out.memory_unreadable = true,
            },
            // No file: the memory controller is not enabled at this level.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            // The file exists (or the directory does not allow a look) but
            // could not be read: the limit is unknown, not absent.
            Err(_) => out.memory_unreadable = true,
        }
        if let Some(c) = std::fs::read_to_string(dir.join("cpu.max"))
            .ok()
            .as_deref()
            .and_then(parse_cpu_max)
        {
            out.cpu_cores = Some(out.cpu_cores.map_or(c, |p| p.min(c)));
        }
        if dir == root || !dir.pop() || !dir.starts_with(root) {
            break;
        }
    }
    out
}

/// The path of the cgroup v1 hierarchy that has `controller` in a
/// `/proc/<pid>/cgroup` text (`<id>:<controllers>:<path>`).
fn v1_path<'a>(proc_cgroup: &'a str, controller: &str) -> Option<&'a str> {
    proc_cgroup.lines().find_map(|l| {
        let mut it = l.splitn(3, ':');
        let id = it.next()?;
        let ctrls = it.next()?;
        let path = it.next()?;
        (id != "0" && ctrls.split(',').any(|c| c == controller)).then(|| path.trim())
    })
}

/// Where the cgroup v1 hierarchy with `controller` is mounted, from a
/// `/proc/self/mountinfo` text.
fn v1_mount(mountinfo: &str, controller: &str) -> Option<String> {
    mountinfo.lines().find_map(|l| {
        let (pre, post) = l.split_once(" - ")?;
        let mut post = post.split_whitespace();
        if post.next()? != "cgroup" {
            return None;
        }
        let _source = post.next()?;
        let opts = post.next()?;
        opts.split(',').any(|o| o == controller).then_some(())?;
        pre.split_whitespace().nth(4).map(str::to_owned)
    })
}

/// Walk from `path` up to the mount, giving each directory to `f`. Returns
/// false when the leaf directory does not exist.
fn walk_v1(mount: &Path, path: &str, mut f: impl FnMut(&Path)) -> bool {
    let mut dir = mount.join(path.trim_start_matches('/'));
    let exists = dir.is_dir();
    loop {
        f(&dir);
        if dir == mount || !dir.pop() || !dir.starts_with(mount) {
            break;
        }
    }
    exists
}

/// The cgroup v1 limits, and whether a memory controller line was present.
fn read_v1(host_root: &Path, proc_cgroup: &str, mountinfo: &str) -> (CgroupLimits, bool) {
    let mut out = CgroupLimits::default();
    let host = |m: String| host_root.join(m.trim_start_matches('/'));
    let mem_path = v1_path(proc_cgroup, "memory");
    if let Some(path) = mem_path.filter(|p| !p.trim_matches('/').is_empty()) {
        match v1_mount(mountinfo, "memory") {
            None => out.memory_unreadable = true,
            Some(m) => {
                let mut read_any = false;
                let exists = walk_v1(&host(m), path, |dir| {
                    let Ok(text) = std::fs::read_to_string(dir.join("memory.limit_in_bytes"))
                    else {
                        return;
                    };
                    match text.trim().parse::<u64>() {
                        Ok(v) if v >= V1_UNLIMITED_FROM => read_any = true,
                        Ok(v) => {
                            read_any = true;
                            out.memory_max = Some(out.memory_max.map_or(v, |c| c.min(v)));
                        }
                        Err(_) => out.memory_unreadable = true,
                    }
                });
                if !exists || !read_any {
                    out.memory_unreadable = true;
                }
            }
        }
    }
    if let (Some(path), Some(m)) = (v1_path(proc_cgroup, "cpu"), v1_mount(mountinfo, "cpu")) {
        walk_v1(&host(m), path, |dir| {
            let num = |f: &str| {
                std::fs::read_to_string(dir.join(f))
                    .ok()
                    .and_then(|t| t.trim().parse::<f64>().ok())
            };
            if let (Some(q), Some(p)) = (num("cpu.cfs_quota_us"), num("cpu.cfs_period_us"))
                && q > 0.0
                && p > 0.0
            {
                let c = q / p;
                out.cpu_cores = Some(out.cpu_cores.map_or(c, |o| o.min(c)));
            }
        });
    }
    (out, mem_path.is_some())
}

/// Read the limits that apply to the cgroup in `proc_cgroup` (cgroup v2, or
/// cgroup v1 where v2 gave no memory limit), under `host_root` (`/`), taking the
/// smallest value along the path to the root. A limit that is read and absent
/// is no limit; a cgroup whose memory limit cannot be read is reported through
/// `memory_unreadable` instead of as no limit.
pub(crate) fn read_limits_in(host_root: &Path, proc_cgroup: &str, mountinfo: &str) -> CgroupLimits {
    let mut out = match cgroup_path(proc_cgroup) {
        Some(p) => read_v2(&host_root.join("sys/fs/cgroup"), p),
        None => CgroupLimits::default(),
    };
    let (v1, has_memory_line) = read_v1(host_root, proc_cgroup, mountinfo);
    if out.memory_max.is_none() && has_memory_line {
        out.memory_max = v1.memory_max;
        out.memory_unreadable = v1.memory_unreadable;
    }
    if out.cpu_cores.is_none() {
        out.cpu_cores = v1.cpu_cores;
    }
    out
}

/// Limits of this process's own cgroup.
#[cfg(target_os = "linux")]
pub(crate) fn read_own_limits() -> CgroupLimits {
    let text = std::fs::read_to_string("/proc/self/cgroup").unwrap_or_default();
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
    read_limits_in(Path::new("/"), &text, &mountinfo)
}

/// `MemTotal` from a `/proc/meminfo` text, in bytes.
pub(crate) fn parse_mem_total(meminfo: &str) -> Option<u64> {
    let kib: u64 = meminfo
        .lines()
        .find_map(|l| l.strip_prefix("MemTotal:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;
    Some(kib * 1024)
}

/// The knobs the derivation uses. `Default` is production.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Policy {
    pub cpu_ms: u64,
    pub wall_ms: u64,
    pub data_floor: u64,
    pub data_ceiling: u64,
    pub max_gate: usize,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            cpu_ms: DEFAULT_CPU_MS,
            wall_ms: DEFAULT_WALL_MS,
            data_floor: DEFAULT_DATA_FLOOR_BYTES,
            data_ceiling: DEFAULT_DATA_CEILING_BYTES,
            max_gate: MAX_GATE,
        }
    }
}

/// The memory the node and its workers together may reach: 90% of the limit.
pub(crate) fn usable(memory_limit: u64) -> u64 {
    memory_limit / 10 * 9
}

/// The memory a new reservation can still take: what is usable, less the node's
/// resident memory now and less what is already reserved.
pub(crate) fn available(memory_limit: u64, node_rss: u64, reserved: u64) -> u64 {
    usable(memory_limit)
        .saturating_sub(node_rss)
        .saturating_sub(reserved)
}

/// The `RLIMIT_DATA` a worker starting now gets, or `None` when less than the
/// floor is available. For a single worker this is the whole headroom up to the
/// ceiling; with others running, only what they left.
pub(crate) fn worker_cap(
    policy: &Policy,
    memory_limit: u64,
    node_rss: u64,
    reserved: u64,
) -> Option<u64> {
    let free = available(memory_limit, node_rss, reserved);
    (free >= policy.data_floor).then(|| free.min(policy.data_ceiling))
}

/// Wall limit for one worker: a quota below one core stretches wall time for the
/// same CPU time.
pub(crate) fn wall_ms(policy: &Policy, cores: Option<f64>) -> u64 {
    match cores {
        Some(c) if c > 0.0 && c < 1.0 => (policy.wall_ms as f64 / c) as u64,
        _ => policy.wall_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1 << 30;
    const MIB: u64 = 1 << 20;

    #[test]
    fn parses_cgroup_files() {
        assert_eq!(parse_memory_max("2147483648\n"), Some(2 * GIB));
        assert_eq!(parse_memory_max("max\n"), None);
        assert_eq!(parse_memory_max("garbage"), None);
        assert_eq!(parse_cpu_max("200000 100000\n"), Some(2.0));
        assert_eq!(parse_cpu_max("max 100000\n"), None);
        assert_eq!(parse_cpu_max("50000 100000"), Some(0.5));
        assert_eq!(parse_cpu_max(""), None);
        assert_eq!(
            cgroup_path("0::/user.slice/user-1000.slice/x.service\n"),
            Some("/user.slice/user-1000.slice/x.service")
        );
        assert_eq!(cgroup_path("12:cpu:/foo\n"), None);
        assert_eq!(
            parse_mem_total("MemTotal:       16384 kB\nMemFree: 1 kB\n"),
            Some(16384 * 1024)
        );
    }

    fn fixture(name: &str) -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!("fncw-cg-{name}-{}", std::process::id()));
        drop(std::fs::remove_dir_all(&root));
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    fn put(root: &Path, rel: &str, text: &str) {
        let p = root.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, text).unwrap();
    }

    const V1_PROC: &str = "4:memory:/svc\n3:cpu,cpuacct:/svc\n1:name=systemd:/svc\n";
    const V1_MOUNTS: &str = "30 25 0:26 / /sys/fs/cgroup/memory rw,nosuid - cgroup cgroup rw,memory\n\
31 25 0:27 / /sys/fs/cgroup/cpu,cpuacct rw,nosuid - cgroup cgroup rw,cpu,cpuacct\n";

    #[test]
    fn v2_reads_the_smallest_limit_along_the_path() {
        let root = fixture("v2");
        let cg = root.join("sys/fs/cgroup");
        put(&cg, "a/memory.max", "1073741824\n");
        put(&cg, "a/b/memory.max", "max\n");
        put(&cg, "a/b/cpu.max", "200000 100000\n");
        put(&cg, "a/cpu.max", "50000 100000\n");
        let got = read_limits_in(&root, "0::/a/b\n", "");
        assert_eq!(got.memory_max, Some(GIB));
        assert_eq!(got.cpu_cores, Some(0.5));
        assert!(!got.memory_unreadable);
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn v2_limit_max_is_unlimited_not_unreadable() {
        let root = fixture("v2max");
        put(&root.join("sys/fs/cgroup"), "a/memory.max", "max\n");
        let got = read_limits_in(&root, "0::/a\n", "");
        assert_eq!(got, CgroupLimits::default());
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn v1_limited_reads_memory_and_cpu() {
        let root = fixture("v1lim");
        let cg = root.join("sys/fs/cgroup");
        put(&cg, "memory/svc/memory.limit_in_bytes", "2147483648\n");
        put(&cg, "memory/memory.limit_in_bytes", "9223372036854771712\n");
        put(&cg, "cpu,cpuacct/svc/cpu.cfs_quota_us", "150000\n");
        put(&cg, "cpu,cpuacct/svc/cpu.cfs_period_us", "100000\n");
        let got = read_limits_in(&root, V1_PROC, V1_MOUNTS);
        assert_eq!(got.memory_max, Some(2 * GIB));
        assert_eq!(got.cpu_cores, Some(1.5));
        assert!(!got.memory_unreadable);
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn v1_unlimited_sentinel_and_no_cpu_quota_are_no_limit() {
        let root = fixture("v1unl");
        let cg = root.join("sys/fs/cgroup");
        put(
            &cg,
            "memory/svc/memory.limit_in_bytes",
            "9223372036854771712\n",
        );
        put(&cg, "cpu,cpuacct/svc/cpu.cfs_quota_us", "-1\n");
        put(&cg, "cpu,cpuacct/svc/cpu.cfs_period_us", "100000\n");
        let got = read_limits_in(&root, V1_PROC, V1_MOUNTS);
        assert_eq!(got, CgroupLimits::default());
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_cgroup_whose_memory_limit_cannot_be_read_is_reported() {
        // v2: the cgroup directory is missing.
        let root = fixture("unr2");
        std::fs::create_dir_all(root.join("sys/fs/cgroup")).unwrap();
        assert!(read_limits_in(&root, "0::/missing/x\n", "").memory_unreadable);
        // v2: a limit file that does not parse.
        put(&root.join("sys/fs/cgroup"), "bad/memory.max", "garbage\n");
        assert!(read_limits_in(&root, "0::/bad\n", "").memory_unreadable);
        // v1: memory controller listed, no mount for it.
        assert!(read_limits_in(&root, V1_PROC, "").memory_unreadable);
        // v1: mounted, but the limit file is absent.
        std::fs::create_dir_all(root.join("sys/fs/cgroup/memory/svc")).unwrap();
        assert!(read_limits_in(&root, V1_PROC, V1_MOUNTS).memory_unreadable);
        // v1: limit file that does not parse.
        put(
            &root.join("sys/fs/cgroup"),
            "memory/svc/memory.limit_in_bytes",
            "x\n",
        );
        assert!(read_limits_in(&root, V1_PROC, V1_MOUNTS).memory_unreadable);
        // Not in any cgroup the process can name: no limit, not unreadable.
        assert_eq!(
            read_limits_in(&root, "nonsense", ""),
            CgroupLimits::default()
        );
        assert_eq!(read_limits_in(&root, "0::/\n", ""), CgroupLimits::default());
        std::fs::remove_dir_all(&root).unwrap();
    }

    /// A cgroup directory that is there but whose limit file is absent (the
    /// controller is not enabled) differs from one whose file cannot be read.
    #[test]
    fn an_absent_memory_file_is_not_unreadable_but_a_failed_read_is() {
        let root = fixture("absent-vs-fail");
        let cg = root.join("sys/fs/cgroup");
        // Directory present, no memory.max anywhere: no limit, readable.
        std::fs::create_dir_all(cg.join("svc")).unwrap();
        assert_eq!(
            read_limits_in(&root, "0::/svc\n", ""),
            CgroupLimits::default()
        );
        // memory.max exists but the read fails (it is a directory here).
        std::fs::create_dir_all(cg.join("svc/memory.max")).unwrap();
        assert!(read_limits_in(&root, "0::/svc\n", "").memory_unreadable);
        std::fs::remove_dir(cg.join("svc/memory.max")).unwrap();
        // The same for an ancestor that carries the limit.
        std::fs::create_dir_all(cg.join("memory.max")).unwrap();
        assert!(read_limits_in(&root, "0::/svc\n", "").memory_unreadable);
        std::fs::remove_dir(cg.join("memory.max")).unwrap();
        // Malformed content.
        put(&cg, "svc/memory.max", "12 garbage\n");
        assert!(read_limits_in(&root, "0::/svc\n", "").memory_unreadable);
        // A readable limit is read, and "max" is no limit.
        put(&cg, "svc/memory.max", "max\n");
        assert_eq!(
            read_limits_in(&root, "0::/svc\n", ""),
            CgroupLimits::default()
        );
        put(&cg, "svc/memory.max", "1073741824\n");
        let got = read_limits_in(&root, "0::/svc\n", "");
        assert_eq!((got.memory_max, got.memory_unreadable), (Some(GIB), false));
        std::fs::remove_dir_all(&root).unwrap();
    }

    /// A permission-denied limit file is a failed read. Skipped where the test
    /// runs with the right to read it regardless (root).
    #[cfg(unix)]
    #[test]
    fn a_permission_denied_memory_file_is_unreadable() {
        use std::os::unix::fs::PermissionsExt;
        let root = fixture("denied");
        let cg = root.join("sys/fs/cgroup");
        put(&cg, "svc/memory.max", "1073741824\n");
        let f = cg.join("svc/memory.max");
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o000)).unwrap();
        if std::fs::read_to_string(&f).is_err() {
            assert!(read_limits_in(&root, "0::/svc\n", "").memory_unreadable);
        }
        std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn the_cap_is_the_whole_headroom_up_to_the_ceiling() {
        let p = Policy::default();
        // 8 GiB unit, 1 GiB node: plenty, so the ceiling.
        assert_eq!(worker_cap(&p, 8 * GIB, GIB, 0), Some(p.data_ceiling));
        // The default 2 GiB unit with a 600 MiB node: about 1.2 GiB usable, so
        // one worker at the ceiling (no division by four).
        assert_eq!(worker_cap(&p, 2 * GIB, 600 * MIB, 0), Some(p.data_ceiling));
        // The same unit with a 1.1 GiB node: the headroom itself, above the floor.
        let cap = worker_cap(&p, 2 * GIB, 1100 * MIB, 0).expect("above the floor");
        assert!(cap >= p.data_floor && cap < p.data_ceiling, "{cap}");
        assert_eq!(cap, usable(2 * GIB) - 1100 * MIB);
        // A node so large that less than the floor is left: no worker.
        assert_eq!(worker_cap(&p, 2 * GIB, 1500 * MIB, 0), None);
        // Nothing left at all, and no underflow.
        assert_eq!(worker_cap(&p, 2 * GIB, 3 * GIB, 0), None);
        // What other workers reserved is not available.
        assert_eq!(
            worker_cap(&p, 8 * GIB, GIB, 7 * GIB),
            None,
            "7 GiB reserved of 6.2 usable"
        );
        assert_eq!(available(4 * GIB, GIB, GIB), usable(4 * GIB) - 2 * GIB);
    }

    #[test]
    fn the_floor_is_one_and_a_half_times_the_worst_real_need() {
        assert_eq!(DEFAULT_DATA_FLOOR_BYTES, 416 * MIB * 3 / 2);
    }

    #[test]
    fn a_half_core_quota_doubles_the_wall_cap() {
        let p = Policy::default();
        assert_eq!(wall_ms(&p, Some(0.5)), 2 * p.wall_ms);
        assert_eq!(wall_ms(&p, Some(2.0)), p.wall_ms);
        assert_eq!(wall_ms(&p, None), p.wall_ms);
    }
}
