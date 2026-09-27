//! End-to-end tests of `freenet open`, the `freenet://` scheme handler (#5726),
//! run against the real binary.
//!
//! Any website can fire a `freenet://` link, so these drive the binary the way
//! an OS launcher does and check that hostile links are refused and valid ones
//! open exactly the fixed loopback URL. On Windows they also take the command
//! line from the registry registration, substitute hostile links for `%1`, and
//! launch it raw, so the real Windows argument parser is exercised: that is the
//! argument-injection surface.
//!
//! `FREENET_OPEN_DRY_RUN=1` makes the handler print what it would open instead
//! of launching a browser.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// The binary Cargo built for this test run (not a hand-rolled target path,
/// which could be a stale binary from an earlier build).
const FREENET_BIN: &str = env!("CARGO_BIN_EXE_freenet");

const RIVER: &str = "raAqMhMG7KUpXBU2SxgCQ3Vh4PYjttxdSWd9ftV7RLv";

/// An isolated environment for one handler run: its own temp, home, config
/// and cache directories, and a stand-in node answering `GET /v1/version`
/// like Freenet (the handler checks it is talking to Freenet).
struct Sandbox {
    _dir: tempfile::TempDir,
    root: PathBuf,
    port: u16,
}

/// Serve `{"version":"test"}` to every connection, for the life of the test
/// process.
fn spawn_fake_node() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind a stand-in node");
    let port = listener.local_addr().expect("local addr").port();
    std::thread::spawn(move || {
        use std::io::{Read, Write};
        for conn in listener.incoming() {
            let Ok(mut conn) = conn else { continue };
            let mut buf = [0u8; 2048];
            drop(conn.read(&mut buf));
            drop(conn.write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{\"version\":\"test\"}",
            ));
        }
    });
    port
}

impl Sandbox {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().to_path_buf();
        let port = spawn_fake_node();
        let config = format!("ws-api-port = {port}\n");
        // Debug builds read `<temp>/freenet/config.toml`; release builds read
        // the platform config dir, which HOME / XDG_CONFIG_HOME redirect on
        // Unix. Write both.
        for dir in config_dirs(&root) {
            std::fs::create_dir_all(&dir).expect("config dir");
            std::fs::write(dir.join("config.toml"), &config).expect("write config");
        }
        Sandbox {
            _dir: dir,
            root,
            port,
        }
    }

    fn port(&self) -> u16 {
        self.port
    }

    fn command(&self) -> Command {
        let mut cmd = Command::new(FREENET_BIN);
        self.apply_env(&mut cmd);
        cmd
    }

    /// Point every directory the handler reads or writes into the sandbox.
    fn apply_env(&self, cmd: &mut Command) {
        let tmp = self.root.join("tmp");
        std::fs::create_dir_all(&tmp).expect("tmp dir");
        cmd.env("FREENET_OPEN_DRY_RUN", "1")
            .env("TMPDIR", &tmp)
            .env("TMP", &tmp)
            .env("TEMP", &tmp)
            .env("HOME", self.root.join("home"))
            .env("XDG_CONFIG_HOME", self.root.join("home/.config"))
            .env("XDG_CACHE_HOME", self.root.join("home/.cache"))
            .env_remove("WS_API_PORT");
    }

    fn open(&self, link: &str) -> Output {
        self.command()
            .args(["open", "--", link])
            .output()
            .expect("run freenet open")
    }

    /// Whether the handler can see this sandbox's config. A Windows release
    /// build reads a known folder no environment variable redirects.
    fn port_is_controllable() -> bool {
        cfg!(debug_assertions) || !cfg!(windows)
    }
}

fn config_dirs(root: &Path) -> Vec<PathBuf> {
    vec![
        root.join("tmp/freenet"),
        root.join("home/.config/freenet"),
        root.join("home/Library/Application Support/The-Freenet-Project-Inc.Freenet"),
    ]
}

/// The handler's local fallback page: `<cache>/freenet/open-link-*.html`.
fn is_fallback_page(path: &str) -> bool {
    Path::new(path)
        .file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.starts_with("open-link-") && n.ends_with(".html"))
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).trim().to_string()
}

#[test]
fn valid_links_open_exactly_the_loopback_url() {
    let sb = Sandbox::new();
    if !Sandbox::port_is_controllable() {
        eprintln!("skipped: cannot redirect the config dir for a Windows release build");
        return;
    }
    let port = sb.port();
    for rest in [
        "",
        "/",
        "/?invitation=abc&x=y",
        "/#store=ABCDEFGHJKLMNPQR",
        "/a/b.html?q=1#frag/../x",
        "/^&calc|x",
    ] {
        // Both forms: the authority form the /open page emits today, and the
        // authority-less form, whose id no desktop can lowercase as a host.
        for link in [
            format!("freenet://{RIVER}{rest}"),
            format!("freenet:{RIVER}{rest}"),
        ] {
            let out = sb.open(&link);
            assert_eq!(out.status.code(), Some(0), "{link}: {out:?}");
            assert_eq!(
                stdout(&out),
                format!("http://127.0.0.1:{port}/v1/contract/web/{RIVER}{rest}"),
                "{link}"
            );
        }
    }
}

#[test]
fn hostile_links_are_refused_without_opening_the_node() {
    let sb = Sandbox::new();
    for link in [
        format!("freenet://{RIVER}/%2e%2e/%2e%2e/"),
        format!("freenet://{RIVER}/../other/"),
        format!("freenet://{RIVER}//evil.example/"),
        format!("freenet:{RIVER}/%2e%2e/"),
        format!("freenet:/{RIVER}/"),
        format!("freenet:///{RIVER}/"),
        format!("freenet://{RIVER}/\" --config-dir \"/tmp/x"),
        format!("freenet://{RIVER}/a b"),
        format!("http://{RIVER}/"),
        "javascript:alert(1)".to_string(),
        "file:///etc/passwd".to_string(),
        "freenet://evil.example/".to_string(),
        "--help".to_string(),
        "--version".to_string(),
        String::new(),
    ] {
        let out = sb.open(&link);
        assert_eq!(out.status.code(), Some(2), "{link:?}: {out:?}");
        let printed = stdout(&out);
        // The dry run prints what it would open: the local "invalid link"
        // page, never a node URL.
        assert!(is_fallback_page(&printed), "{link:?} opened {printed:?}");
        assert!(
            !printed.contains("127.0.0.1"),
            "{link:?} opened {printed:?}"
        );
    }
}

#[test]
fn more_than_one_positional_is_refused() {
    let sb = Sandbox::new();
    let out = sb
        .command()
        .args([
            "open",
            "--",
            &format!("freenet://{RIVER}/"),
            "--config-dir",
            "x",
        ])
        .output()
        .expect("run");
    assert_eq!(out.status.code(), Some(2), "{out:?}");
    assert!(!stdout(&out).contains("127.0.0.1"));
}

#[test]
fn not_running_node_gets_the_explanatory_page() {
    let sb = Sandbox::new();
    if !Sandbox::port_is_controllable() {
        return;
    }
    // Point the handler at a port where something listens but is not Freenet
    // (it never answers): the handler must treat the node as not running.
    let squatter = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = squatter.local_addr().expect("addr").port();
    for dir in config_dirs(&sb.root) {
        std::fs::write(dir.join("config.toml"), format!("ws-api-port = {port}\n"))
            .expect("write config");
    }
    let out = sb.open(&format!("freenet://{RIVER}/#invite=SECRET-TOKEN"));
    drop(squatter);
    assert_eq!(out.status.code(), Some(1), "{out:?}");
    let page_path = stdout(&out);
    assert!(is_fallback_page(&page_path), "{page_path}");
    let page = std::fs::read_to_string(&page_path).expect("read page");
    assert!(page.contains("Freenet isn't running"));
    assert!(page.contains(&format!(
        "href=\"http://127.0.0.1:{port}/v1/contract/web/{RIVER}/#invite=SECRET-TOKEN\""
    )));
    // Stderr from a browser-launched handler usually lands in the journal:
    // it must not carry the link, whose fragment can be a secret.
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!stderr.contains("SECRET-TOKEN"), "{stderr}");
    assert!(!stderr.contains(RIVER), "{stderr}");
}

/// The Windows argument-injection surface, exercised for real: register, read
/// the command line back from the registry, substitute each link for `%1`
/// exactly as Explorer does, and launch it raw.
///
/// It writes the real `HKCU\Software\Classes\freenet` of whoever runs it, so
/// it only runs where `CI` is set (a disposable runner), never on a
/// developer's machine with Freenet installed.
#[cfg(windows)]
#[test]
fn windows_registered_command_line_resists_argument_injection() {
    use std::os::windows::process::CommandExt;

    if std::env::var_os("CI").is_none() {
        eprintln!("skipped: modifies the real HKCU registration; set CI=1 to run");
        return;
    }
    let sb = Sandbox::new();
    let register = sb
        .command()
        .args(["service", "url-handler", "register"])
        .output()
        .expect("register");
    assert!(register.status.success(), "{register:?}");

    let query = Command::new("reg")
        .args([
            "query",
            r"HKCU\Software\Classes\freenet\shell\open\command",
            "/ve",
        ])
        .output()
        .expect("reg query");
    let text = String::from_utf8_lossy(&query.stdout).to_string();
    let template = text
        .lines()
        .find_map(|l| l.split_once("REG_SZ").map(|(_, v)| v.trim().to_string()))
        .unwrap_or_else(|| panic!("no command registered: {text}"));
    assert!(template.ends_with(r#" open -- "%1""#), "{template}");
    let (exe, args) = {
        let rest = template.strip_prefix('"').expect("quoted exe");
        let (exe, args) = rest.split_once('"').expect("closing quote");
        (exe.to_string(), args.to_string())
    };
    assert!(
        Path::new(&exe).eq(Path::new(FREENET_BIN)) || exe.eq_ignore_ascii_case(FREENET_BIN),
        "registered {exe}, built {FREENET_BIN}"
    );

    // Explorer substitutes the link for %1 in the registered string and
    // passes the result to CreateProcess; `raw_arg` hands Windows the
    // argument text untouched, so the real argv parser splits it.
    let run = |link: &str| -> Output {
        let mut cmd = Command::new(&exe);
        sb.apply_env(&mut cmd);
        cmd.raw_arg(args.replace("%1", link).trim_start())
            .output()
            .expect("launch registered command")
    };

    if Sandbox::port_is_controllable() {
        let port = sb.port();
        for rest in ["/", "/?a=1&b=2#c", "/^&calc", "/a|b"] {
            let link = format!("freenet://{RIVER}{rest}");
            let out = run(&link);
            assert_eq!(out.status.code(), Some(0), "{link}: {out:?}");
            assert_eq!(
                stdout(&out),
                format!("http://127.0.0.1:{port}/v1/contract/web/{RIVER}{rest}")
            );
        }
    }

    for link in [
        // Close the quote and append a flag and a second argument.
        format!(r#"freenet://{RIVER}/" --config-dir "C:\x"#),
        // Backslash-escaped quote: the parser yields a literal quote.
        format!(r#"freenet://{RIVER}/\" x"#),
        // Close the quote and start a second link.
        format!(r#"freenet://{RIVER}/" "freenet://{RIVER}/"#),
        format!(r#"freenet://{RIVER}/" --help"#),
        format!("freenet://{RIVER}/%2e%2e/"),
        format!("http://{RIVER}/"),
    ] {
        let out = run(&link);
        assert_eq!(out.status.code(), Some(2), "{link}: {out:?}");
        assert!(!stdout(&out).contains("127.0.0.1"), "{link}: {out:?}");
    }

    let unregister = sb
        .command()
        .args(["service", "url-handler", "unregister"])
        .output()
        .expect("unregister");
    assert!(unregister.status.success(), "{unregister:?}");
    let gone = Command::new("reg")
        .args(["query", r"HKCU\Software\Classes\freenet"])
        .output()
        .expect("reg query");
    assert!(!gone.status.success(), "key still present after unregister");
}
