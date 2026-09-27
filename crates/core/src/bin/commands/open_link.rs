//! `freenet open`: the `freenet://` URL scheme handler (#5726).
//!
//! A `freenet://<contract-id><rest>` link is what the "Open in Freenet" button on
//! <https://freenet.org/open> produces. The OS hands it to this handler, which
//! opens the user's browser at the local peer:
//! `http://127.0.0.1:<port>/v1/contract/web/<contract-id><rest>`.
//!
//! Any website can fire a `freenet://` link, so everything here treats the input
//! as hostile:
//!
//! * Validation reproduces the /open page's JS exactly (freenet/web,
//!   `hugo-site/themes/freenet/layouts/shortcodes/open-link.html`). The rules
//!   are pinned by `tests/data/share-link-vectors.json`: this crate's tests run
//!   every vector, the page's JS was checked against the same file when the
//!   handler was written, and freenet/web carries a CI check over a copy of it
//!   (#5726 follow-up). Change a rule on both sides together with the file.
//! * The output URL is a fixed `http://127.0.0.1:<port>/v1/contract/web/` prefix
//!   plus the validated id and rest. Nothing in the input can choose the scheme,
//!   host, port or contract.
//! * The URL is handed to the browser opener as a single argument (`xdg-open`,
//!   `open`) or wide string (`ShellExecuteW`), never through a shell.
//! * The OS registrations put the link after a literal `--`
//!   (`open -- %u` / `open -- "%1"`), and exactly one positional is accepted, so
//!   argument injection cannot smuggle in a flag or a second argument.
//!
//! Two link forms are accepted: `freenet://<id><rest>` (what the /open page
//! emits today) and the authority-less `freenet:<id><rest>` (the id must
//! follow the colon directly; `freenet:/<id>` is refused, and `freenet:///<id>`
//! is the authority form with an empty id, so it is refused too). The second exists
//! because some desktops re-parse the link before handing it over and
//! lowercase a URL's host, which in the first form is the case-sensitive
//! contract id (Qt's `QUrl`, used by KDE's `kde-open`, does this). The
//! authority-less form has no host to lowercase.
//!
//! The handler itself never starts a stopped node: that would interact with
//! the service supervisors (systemd's start limit, a wrapper mid-update). When
//! the node is not reachable it opens a local page that says so instead of
//! failing silently. On macOS, LaunchServices launches `Freenet.app` to
//! deliver a link if it is not running, exactly as a double-click would; the
//! in-app handler then waits for the node it just started.
//!
//! The port comes from `ws-api-port` in the node's config (`--config-dir` /
//! `CONFIG_DIR` if given, else the default config directory). The URL is
//! always `127.0.0.1`, so a node bound only to a specific non-loopback address
//! reads as "not running".

use anyhow::{Context, Result, bail};
use clap::Args;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Default HTTP/WebSocket API port, used when the user's config does not set
/// `ws-api-port`. Mirrors `default_ws_api_port()` in the library config.
pub const DEFAULT_PORT: u16 = 7509;

/// When set (to anything non-empty), the handler prints the URL it would open
/// instead of launching a browser. Used by CI to run the exact registered
/// command line. It can only suppress a browser launch, so it adds no attack
/// surface.
pub const DRY_RUN_ENV_VAR: &str = "FREENET_OPEN_DRY_RUN";

/// How long the macOS in-app handler waits for the node: LaunchServices may
/// have just launched `Freenet.app` in order to deliver the link, so the node
/// can still be starting.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
pub const APP_LAUNCH_NODE_WAIT: Duration = Duration::from_secs(30);

/// How long the OS-launched CLI handler polls for the node before showing the
/// "not running" page.
const CLI_NODE_WAIT: Duration = Duration::from_secs(3);

/// Exit code for a link that failed validation.
pub const EXIT_CODE_INVALID_LINK: i32 = 2;

// ── Validation (must match freenet.org/open) ────────────────────────────────

/// Longest contract-id candidate accepted before decoding. A 32-byte id is at
/// most 44 base58 characters; the page uses the same generous ceiling.
const MAX_CANDIDATE_ID_LEN: usize = 64;
/// Longest accepted path/query/app-fragment after the id.
const MAX_REST_LEN: usize = 2000;
/// Contract ids are 32 bytes.
const CONTRACT_KEY_BYTES: usize = 32;

/// A validated share-link target: the contract id plus everything after it,
/// both exactly as they appeared in the link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareTarget {
    contract_id: String,
    rest: String,
}

impl ShareTarget {
    /// The local gateway path: `/v1/contract/web/<id><rest>`.
    pub fn local_path(&self) -> String {
        format!("/v1/contract/web/{}{}", self.contract_id, self.rest)
    }

    /// The URL to open: `http://127.0.0.1:<port>/v1/contract/web/<id><rest>`.
    pub fn local_url(&self, port: u16) -> String {
        format!("http://127.0.0.1:{port}{}", self.local_path())
    }
}

/// Why a link was refused. Deliberately carries no attacker-controlled text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkError {
    NotFreenetScheme,
    InvalidContractId,
    InvalidRest,
}

impl std::fmt::Display for LinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            LinkError::NotFreenetScheme => "not a freenet:// link",
            LinkError::InvalidContractId => "the link does not name a valid contract id",
            LinkError::InvalidRest => "the link's path, query or fragment is not allowed",
        })
    }
}

/// Parse a full link: `freenet://<id><rest>` or `freenet:<id><rest>`. The
/// scheme is matched case-insensitively (RFC 3986). In the authority-less
/// form the id must follow the colon directly, so `freenet:/x` and
/// `freenet:///x` are refused rather than guessed at.
pub fn parse_freenet_link(link: &str) -> Result<ShareTarget, LinkError> {
    let scheme = "freenet:";
    let rest = match link.get(..scheme.len()) {
        Some(p) if p.eq_ignore_ascii_case(scheme) => &link[scheme.len()..],
        _ => return Err(LinkError::NotFreenetScheme),
    };
    match rest.strip_prefix("//") {
        Some(authority_form) => {
            let target = parse_share_fragment(authority_form)?;
            // In this form the id is the URL's host, and some desktops
            // lowercase hosts (KDE via Qt's QUrl). About a quarter of
            // lowercased ids still decode to a valid, DIFFERENT id, so the
            // round trip alone would open the wrong contract. A real id with
            // no uppercase letter is vanishingly unlikely (about 1 in 10^10
            // for 44 characters), so treat one as a lowercased link.
            if !target.contract_id.bytes().any(|b| b.is_ascii_uppercase()) {
                return Err(LinkError::InvalidContractId);
            }
            Ok(target)
        }
        None if rest.starts_with('/') => Err(LinkError::NotFreenetScheme),
        None => parse_share_fragment(rest),
    }
}

/// Validate `<contract-id><rest>`, the part of a share link after
/// `https://freenet.org/open#` or `freenet://`. Port of the page's
/// `splitFragment` + `parseContractId` + `validateRest`.
pub fn parse_share_fragment(raw: &str) -> Result<ShareTarget, LinkError> {
    if raw.is_empty() {
        return Err(LinkError::InvalidContractId);
    }
    let split = raw.find(['/', '?', '#']).unwrap_or(raw.len());
    let (id, rest) = raw.split_at(split);
    if !is_valid_contract_id(id) {
        return Err(LinkError::InvalidContractId);
    }
    if !is_valid_rest(rest) {
        return Err(LinkError::InvalidRest);
    }
    Ok(ShareTarget {
        contract_id: id.to_string(),
        rest: rest.to_string(),
    })
}

/// Strict, round-trip validation of a base58 contract id.
///
/// `ContractInstanceId::from_base58` zero-pads short input rather than
/// rejecting it, so a length/charset check is not enough: the decoded bytes
/// must re-encode to the identical text.
///
/// An id made only of `'1'`s is refused. The page's decoder turns 32 `'1'`s
/// into 33 bytes and rejects it, while `bs58` yields the all-zero id; refusing
/// it here keeps the two sides identical (and no real contract hashes to zero).
fn is_valid_contract_id(candidate: &str) -> bool {
    if candidate.is_empty() || candidate.len() > MAX_CANDIDATE_ID_LEN {
        return false;
    }
    // Bitcoin base58 alphabet: no 0, O, I, l.
    if !candidate.bytes().all(|b| {
        matches!(b, b'1'..=b'9' | b'A'..=b'H' | b'J'..=b'N' | b'P'..=b'Z' | b'a'..=b'k' | b'm'..=b'z')
    }) {
        return false;
    }
    if candidate.bytes().all(|b| b == b'1') {
        return false;
    }
    let Ok(decoded) = bs58::decode(candidate)
        .with_alphabet(bs58::Alphabet::BITCOIN)
        .into_vec()
    else {
        return false;
    };
    if decoded.len() != CONTRACT_KEY_BYTES {
        return false;
    }
    bs58::encode(&decoded)
        .with_alphabet(bs58::Alphabet::BITCOIN)
        .into_string()
        == candidate
}

/// Characters allowed after the id: RFC 3986 unreserved + reserved + `%`, plus
/// `{}|^` (left unencoded by browsers in `location.hash`). Mirrors the page's
/// `REST_ALLOWED` regex.
fn is_allowed_rest_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric()
        || matches!(
            b,
            b'-' | b'.'
                | b'_'
                | b'~'
                | b':'
                | b'/'
                | b'?'
                | b'#'
                | b'['
                | b']'
                | b'@'
                | b'!'
                | b'$'
                | b'&'
                | b'\''
                | b'('
                | b')'
                | b'*'
                | b'+'
                | b','
                | b';'
                | b'='
                | b'%'
                | b'{'
                | b'}'
                | b'|'
                | b'^'
        )
}

fn is_valid_rest(rest: &str) -> bool {
    if rest.len() > MAX_REST_LEN {
        return false;
    }
    let bytes = rest.as_bytes();
    if !bytes.iter().copied().all(is_allowed_rest_byte) {
        return false;
    }
    // A "//host" path could be read as a new authority by some parsers.
    if rest.starts_with("//") {
        return false;
    }
    // Every '%' must start a well-formed escape that does not encode a control
    // character. Like the page, each '%' is checked in place (no skipping).
    for (i, &b) in bytes.iter().enumerate() {
        if b != b'%' {
            continue;
        }
        let Some(hex) = rest.get(i + 1..i + 3) else {
            return false;
        };
        // Check the digits first: `from_str_radix` would accept a leading '+',
        // which the page's /^[0-9A-Fa-f]{2}$/ does not.
        if !hex.bytes().all(|h| h.is_ascii_hexdigit()) {
            return false;
        }
        let Ok(code) = u8::from_str_radix(hex, 16) else {
            return false;
        };
        if code <= 0x1f || code == 0x7f {
            return false;
        }
    }
    // No '.' / '..' path segment, literal or percent-encoded, before the first
    // '?' or '#'. Left unchecked, `%2e%2e/<other-id>/` resolves in the browser
    // to a DIFFERENT contract than the link names.
    let path_part = rest.split(['?', '#']).next().unwrap_or("");
    !path_part.split('/').any(is_dot_segment)
}

fn is_dot_segment(segment: &str) -> bool {
    let s = segment.to_ascii_lowercase();
    matches!(s.as_str(), "." | "%2e" | ".." | ".%2e" | "%2e." | "%2e%2e")
}

// ── Local node discovery ────────────────────────────────────────────────────

/// The node's default config directory, resolved read-only.
///
/// Mirrors `ConfigPathsArgs::default_dirs` in the library (same `ProjectDirs`
/// constants in release builds, the temp dir in debug builds) without calling
/// it: that function creates and removes directories, and `ConfigArgs::build()`
/// fetches gateways over the network. A URL handler must do neither.
///
/// Debug builds only: a node started with `--id` keeps its config under
/// `<temp>/freenet-<id>`, which this does not know; pass `--config-dir`.
fn default_config_dir() -> Option<PathBuf> {
    // `CONFIG_DIR` is the node's own override (ConfigPathsArgs), honoured
    // here too so the in-app macOS handler, which has no command line,
    // agrees with a node started with it.
    if let Some(dir) = std::env::var_os("CONFIG_DIR").filter(|d| !d.is_empty()) {
        return Some(PathBuf::from(dir));
    }
    if cfg!(debug_assertions) {
        Some(std::env::temp_dir().join("freenet"))
    } else {
        // `config_local_dir`, as `ConfigArgs::build` uses: on Windows that is
        // Local AppData, not the Roaming `config_dir`.
        directories::ProjectDirs::from("", "The Freenet Project Inc", "Freenet")
            .map(|d| d.config_local_dir().to_path_buf())
    }
}

/// `ws-api-port` from a `config.toml` body, if present and valid.
fn port_from_config_toml(content: &str) -> Option<u16> {
    let value: toml::Value = toml::from_str(content).ok()?;
    let port = value.get("ws-api-port")?.as_integer()?;
    u16::try_from(port).ok().filter(|p| *p != 0)
}

/// The port the local node serves its HTTP API on: `ws-api-port` from the
/// config in `config_dir` (or the default config directory), else
/// [`DEFAULT_PORT`].
pub fn configured_port(config_dir: Option<&Path>) -> u16 {
    config_dir
        .map(Path::to_path_buf)
        .or_else(default_config_dir)
        .and_then(|dir| std::fs::read_to_string(dir.join("config.toml")).ok())
        .and_then(|content| port_from_config_toml(&content))
        .unwrap_or(DEFAULT_PORT)
}

/// Whether a Freenet node answers on `127.0.0.1:<port>`, polling until `wait`
/// has elapsed (at least one attempt is always made).
pub fn node_is_listening(port: u16, wait: Duration) -> bool {
    poll_until(wait, Duration::from_millis(500), || {
        freenet_answers_on(port)
    })
}

/// Call `probe` until it returns true or `wait` has elapsed, sleeping
/// `interval` between attempts. At least one attempt is always made.
fn poll_until(wait: Duration, interval: Duration, mut probe: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + wait;
    loop {
        if probe() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(interval);
    }
}

/// Whether a FREENET node answers on `127.0.0.1:<port>`, not merely whether
/// something accepts connections there. With Freenet stopped, an unrelated
/// local service on the port would otherwise receive the link, and with it any
/// secret in its path or query. Asks `GET /v1/version` and expects the node's
/// JSON (`{"version":"..."}`). This keeps accidental collisions out; a local
/// process deliberately imitating the node is outside what a probe can stop.
fn freenet_answers_on(port: u16) -> bool {
    use std::io::{Read, Write};
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let timeout = Duration::from_millis(1500);
    let Ok(mut stream) = std::net::TcpStream::connect_timeout(&addr, timeout) else {
        return false;
    };
    if stream.set_read_timeout(Some(timeout)).is_err()
        || stream.set_write_timeout(Some(timeout)).is_err()
    {
        return false;
    }
    let request = format!(
        "GET /v1/version HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAccept: application/json\r\n\
         Connection: close\r\n\r\n"
    );
    if stream.write_all(request.as_bytes()).is_err() {
        return false;
    }
    let mut response = Vec::new();
    // The node's reply is ~150 bytes; cap what a stranger can make us read.
    drop(stream.take(8192).read_to_end(&mut response));
    looks_like_freenet_version_response(&String::from_utf8_lossy(&response))
}

/// Pure check of a raw HTTP response to `GET /v1/version`.
fn looks_like_freenet_version_response(response: &str) -> bool {
    let Some((head, body)) = response.split_once("\r\n\r\n") else {
        return false;
    };
    let status_ok = head
        .lines()
        .next()
        .is_some_and(|l| l.starts_with("HTTP/1.1 200") || l.starts_with("HTTP/1.0 200"));
    status_ok
        && serde_json::from_str::<serde_json::Value>(body.trim())
            .ok()
            .and_then(|v| v.get("version")?.as_str().map(str::to_owned))
            .is_some()
}

// ── Opening things ──────────────────────────────────────────────────────────

fn dry_run() -> bool {
    std::env::var_os(DRY_RUN_ENV_VAR).is_some_and(|v| !v.is_empty())
}

/// Open `target` (an `http://127.0.0.1` URL or a local file path) in the
/// default browser. The target is always a single argument; no shell.
fn launch(target: &str) -> Result<()> {
    if dry_run() {
        println!("{target}");
        return Ok(());
    }
    #[cfg(target_os = "windows")]
    {
        use std::ffi::OsStr;
        use std::os::windows::ffi::OsStrExt;
        let operation: Vec<u16> = OsStr::new("open").encode_wide().chain(Some(0)).collect();
        let target_wide: Vec<u16> = OsStr::new(target).encode_wide().chain(Some(0)).collect();
        // SAFETY: both buffers are NUL-terminated UTF-16 strings that outlive
        // the call; the remaining pointer arguments are documented as optional.
        let result = unsafe {
            winapi::um::shellapi::ShellExecuteW(
                std::ptr::null_mut(),
                operation.as_ptr(),
                target_wide.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                winapi::um::winuser::SW_SHOWNORMAL,
            )
        };
        // ShellExecuteW returns a value greater than 32 on success.
        if result as usize <= 32 {
            bail!("ShellExecuteW failed ({})", result as usize);
        }
        Ok(())
    }
    #[cfg(not(target_os = "windows"))]
    {
        let opener = if cfg!(target_os = "macos") {
            "open"
        } else {
            "xdg-open"
        };
        let mut cmd = std::process::Command::new(opener);
        cmd.arg(target)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        if cfg!(target_os = "macos") {
            // `open` hands off and returns at once. Waiting reaps it, which
            // matters inside the long-lived menu-bar wrapper.
            let status = cmd
                .status()
                .with_context(|| format!("failed to run {opener}"))?;
            if !status.success() {
                bail!("{opener} exited with {status}");
            }
        } else {
            // Not waited on: some xdg-open backends block until the browser
            // exits. This is a short-lived process, so nothing is left behind.
            cmd.spawn()
                .with_context(|| format!("failed to run {opener}"))?;
        }
        Ok(())
    }
}

/// What the local fallback page reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FallbackPage {
    /// The link was valid but no Freenet node answered on the port. Carries no
    /// part of the link: it can hold a secret (e.g. a River invite), and this
    /// page is a file on disk. The user goes back and clicks the link again,
    /// which re-runs the handler, probe included.
    NotRunning { port: u16 },
    /// The link failed validation. Carries no attacker-controlled text.
    InvalidLink,
}

fn start_instructions() -> &'static str {
    if cfg!(target_os = "macos") {
        "Open Freenet from your Applications folder."
    } else if cfg!(target_os = "windows") {
        "Start Freenet by running <code>freenet.exe</code>, or run \
         <code>freenet service start</code> in a terminal."
    } else {
        "Start it with <code>freenet service start</code> (or \
         <code>systemctl --user start freenet</code>)."
    }
}

/// Render the fallback page. Pure, so it is unit-tested. It interpolates
/// nothing from the link (only the port number), so there is nothing to
/// escape.
pub fn render_fallback_page(page: &FallbackPage) -> String {
    let (title, body) = match page {
        FallbackPage::NotRunning { port } => (
            "Freenet isn't running",
            format!(
                "<p>This link opens a Freenet site on your own computer, but no Freenet \
                 peer answered on port {port}.</p>\n<p>{}</p>\n\
                 <p>Then go back and click the link again.</p>",
                start_instructions(),
            ),
        ),
        FallbackPage::InvalidLink => (
            "This Freenet link isn't valid",
            "<p>The <code>freenet://</code> link you followed is malformed, so it was \
             not opened. Ask whoever shared it for a new link.</p>"
                .to_string(),
        ),
    };
    format!(
        "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n\
         <title>{title}</title>\n<style>\n\
         body{{font-family:system-ui,sans-serif;max-width:36rem;margin:4rem auto;padding:0 1rem;\
         line-height:1.5;color:#1a1a1a;background:#fff}}\n\
         @media (prefers-color-scheme:dark){{body{{color:#e6e6e6;background:#141414}}a{{color:#8ab4f8}}}}\n\
         code{{font-size:.95em}}\n.button{{display:inline-block;padding:.5rem 1rem;border:1px solid;\
         border-radius:.4rem;text-decoration:none}}\n</style>\n</head>\n<body>\n<h1>{title}</h1>\n\
         {body}\n<p>Don't have Freenet? <a href=\"https://freenet.org/quickstart/\">Get Freenet</a>.</p>\n\
         </body>\n</html>\n"
    )
}

/// A one-line desktop notification for the fallback cases on Linux. Sandboxed
/// browsers (Ubuntu's snap Firefox and Chromium) cannot read files under a
/// hidden directory such as `~/.cache`, so the page alone may not be seen.
/// Best effort, static text only (never the link).
#[cfg(target_os = "linux")]
fn notify_desktop(page: &FallbackPage) {
    if dry_run() {
        return;
    }
    let body = match page {
        FallbackPage::NotRunning { .. } => {
            "Freenet isn't running. Start it with `freenet service start`, then open the link again."
        }
        FallbackPage::InvalidLink => "That freenet:// link isn't valid, so it was not opened.",
    };
    drop(
        std::process::Command::new("notify-send")
            .args(["--app-name=Freenet", "Freenet", body])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn(),
    );
}

/// Write the fallback page to the user's cache dir and open it.
fn show_fallback_page(page: &FallbackPage) -> Result<()> {
    #[cfg(target_os = "linux")]
    notify_desktop(page);
    let dir = fallback_page_dir().context("no cache directory")?;
    write_and_open_fallback_page(page, &dir, launch)
}

/// Write the page into `dir` and hand it to `open`; a page whose launch
/// fails is removed at once. Split out so the failure path is testable.
fn write_and_open_fallback_page(
    page: &FallbackPage,
    dir: &Path,
    open: impl FnOnce(&str) -> Result<()>,
) -> Result<()> {
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    sweep_stale_fallback_pages_in(dir, FALLBACK_PAGE_LIFETIME);
    // One file per invocation, so concurrent handlers cannot overwrite each
    // other's page. Owner-only (0600 on Unix, via tempfile).
    let file = tempfile::Builder::new()
        .prefix(FALLBACK_PAGE_PREFIX)
        .suffix(".html")
        .tempfile_in(dir)
        .with_context(|| format!("creating a page in {}", dir.display()))?;
    std::io::Write::write_all(&mut file.as_file(), render_fallback_page(page).as_bytes())?;
    let (_, path) = file
        .keep()
        .map_err(|e| anyhow::anyhow!("keeping the page: {e}"))?;
    if let Err(e) = open(&path.to_string_lossy()) {
        drop(std::fs::remove_file(&path));
        return Err(e);
    }
    Ok(())
}

/// Where fallback pages are written: `open-link/` in the node's own
/// ProjectDirs cache directory, which `uninstall --purge` removes. (Not
/// `<cache>/freenet`: on case-insensitive filesystems that is the same folder
/// as macOS's `~/Library/Caches/Freenet`, with the wrapper lock and updater
/// staging, and Windows' `%LOCALAPPDATA%\Freenet`, the install root.)
fn fallback_page_dir() -> Option<PathBuf> {
    directories::ProjectDirs::from("", "The Freenet Project Inc", "Freenet")
        .map(|d| d.cache_dir().join("open-link"))
}

const FALLBACK_PAGE_PREFIX: &str = "open-link-";

/// How long a fallback page is kept. The pages carry no part of the link, so
/// this is only tidiness: the browser must have loaded the page first, and a
/// cold browser start can be slow. Nothing sleeps for this: stale pages are
/// swept by the next fallback page and whenever the node starts
/// ([`sweep_stale_fallback_pages`]).
const FALLBACK_PAGE_LIFETIME: Duration = Duration::from_secs(10 * 60);

/// Delete fallback pages in `dir` older than `max_age`. Best effort.
fn sweep_stale_fallback_pages_in(dir: &Path, max_age: Duration) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !(name.starts_with(FALLBACK_PAGE_PREFIX) && name.ends_with(".html")) {
            continue;
        }
        let stale = entry
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.elapsed().ok())
            .is_some_and(|age| age >= max_age);
        if stale {
            drop(std::fs::remove_file(entry.path()));
        }
    }
}

/// Delete stale fallback pages in the default location. Called when the node
/// starts, so a page is not kept forever when no further link is opened.
pub fn sweep_stale_fallback_pages() {
    if let Some(dir) = fallback_page_dir() {
        sweep_stale_fallback_pages_in(&dir, FALLBACK_PAGE_LIFETIME);
    }
}

#[cfg(target_os = "linux")]
/// Write `contents` to `path` via a temp file in the same directory + rename,
/// so a concurrent reader never sees a partial file.
pub(crate) fn write_atomically(path: &Path, contents: &[u8]) -> Result<()> {
    let dir = path.parent().context("path has no parent")?;
    let mut tmp = tempfile::NamedTempFile::new_in(dir)
        .with_context(|| format!("creating a temp file in {}", dir.display()))?;
    std::io::Write::write_all(&mut tmp, contents)?;
    tmp.persist(path)
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(())
}

/// Result of handling one link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandleOutcome {
    OpenedLocal(String),
    /// The node answered but the browser could not be launched.
    OpenFailed(String),
    NodeNotRunning(String),
    Invalid(LinkError),
}

impl HandleOutcome {
    /// A short label for logs, carrying no part of the link.
    #[cfg_attr(not(target_os = "macos"), allow(dead_code))]
    pub fn kind(&self) -> &'static str {
        match self {
            HandleOutcome::OpenedLocal(_) => "opened",
            HandleOutcome::OpenFailed(_) => "browser-launch-failed",
            HandleOutcome::NodeNotRunning(_) => "node-not-running",
            HandleOutcome::Invalid(_) => "invalid-link",
        }
    }
}

/// Handle one link end to end: validate, find the node, open the browser (or
/// the fallback page). `wait` is how long to poll for the node to come up.
pub fn handle_link(link: &str, wait: Duration, config_dir: Option<&Path>) -> HandleOutcome {
    let target = match parse_freenet_link(link) {
        Ok(t) => t,
        Err(e) => {
            if let Err(err) = show_fallback_page(&FallbackPage::InvalidLink) {
                tracing::warn!(error = %err, "could not show the invalid-link page");
            }
            return HandleOutcome::Invalid(e);
        }
    };
    let port = configured_port(config_dir);
    let local_url = target.local_url(port);
    if node_is_listening(port, wait) {
        match launch(&local_url) {
            Ok(()) => HandleOutcome::OpenedLocal(local_url),
            Err(err) => {
                tracing::warn!(error = %err, "could not open the browser");
                HandleOutcome::OpenFailed(local_url)
            }
        }
    } else {
        let page = FallbackPage::NotRunning { port };
        if let Err(err) = show_fallback_page(&page) {
            tracing::warn!(error = %err, "could not show the not-running page");
        }
        HandleOutcome::NodeNotRunning(local_url)
    }
}

// ── CLI ─────────────────────────────────────────────────────────────────────

/// Open a `freenet://` link in your browser, via your local Freenet peer.
///
/// This is what the operating system runs when you click a `freenet://` link
/// (for example the "Open in Freenet" button on freenet.org/open). It opens
/// `http://127.0.0.1:<port>/v1/contract/web/<contract-id>/...` using the port
/// from your Freenet config.
#[derive(Args, Debug, Clone)]
pub struct OpenCommand {
    /// The freenet:// link. Exactly one; registered handlers pass it after `--`.
    #[arg(
        value_name = "LINK",
        required = true,
        num_args = 1..,
        allow_hyphen_values = true
    )]
    pub links: Vec<OsString>,
}

/// Pick the single link from the positionals. More than one means the OS
/// launcher split a hostile link into several arguments: refuse all of them.
fn single_link(links: &[OsString]) -> Result<&str> {
    match links {
        [one] => one.to_str().context("the link is not valid UTF-8"),
        _ => bail!(
            "expected exactly one freenet:// link, got {} arguments; refusing",
            links.len()
        ),
    }
}

impl OpenCommand {
    /// `config_dir` is `--config-dir` / `CONFIG_DIR` from the top-level
    /// arguments, if given, so a node with a non-default config is found.
    pub fn run(&self, config_dir: Option<&Path>) -> Result<()> {
        #[cfg(target_os = "windows")]
        detach_from_private_console();

        let link = match single_link(&self.links) {
            Ok(link) => link,
            Err(e) => {
                if let Err(err) = show_fallback_page(&FallbackPage::InvalidLink) {
                    eprintln!("Could not show the invalid-link page: {err:#}");
                }
                eprintln!("{e:#}");
                std::process::exit(EXIT_CODE_INVALID_LINK);
            }
        };
        // A short grace period: a link clicked just after login can arrive
        // while the service is still binding its port. (The macOS in-app
        // handler waits longer, as it may have launched the app itself.)
        match handle_link(link, CLI_NODE_WAIT, config_dir) {
            HandleOutcome::OpenedLocal(_) => Ok(()),
            HandleOutcome::OpenFailed(_) => {
                eprintln!("Freenet is running, but the web browser could not be launched.");
                std::process::exit(1);
            }
            HandleOutcome::NodeNotRunning(_) => {
                // Only the port: the link itself can carry secrets in its
                // fragment (e.g. River invites), and stderr from a
                // browser-launched handler usually ends up in the journal.
                eprintln!(
                    "Freenet is not running (nothing answered on 127.0.0.1:{}). Start it \
                     with `freenet service start`, then open the link again.",
                    configured_port(config_dir)
                );
                std::process::exit(1);
            }
            HandleOutcome::Invalid(e) => {
                eprintln!("Refusing to open this link: {e}.");
                std::process::exit(EXIT_CODE_INVALID_LINK);
            }
        }
    }
}

/// A browser-launched handler gets a console window of its own on Windows
/// (freenet.exe is a console program). Release it straight away so it closes;
/// keep it when started from a terminal the user is looking at.
#[cfg(target_os = "windows")]
fn detach_from_private_console() {
    let mut pids = [0u32; 2];
    // SAFETY: the buffer is valid for `pids.len()` u32s.
    let count =
        unsafe { winapi::um::wincon::GetConsoleProcessList(pids.as_mut_ptr(), pids.len() as u32) };
    if count == 1 {
        // SAFETY: FreeConsole has no preconditions.
        unsafe {
            winapi::um::wincon::FreeConsole();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RIVER: &str = "raAqMhMG7KUpXBU2SxgCQ3Vh4PYjttxdSWd9ftV7RLv";

    #[derive(serde::Deserialize)]
    struct Vectors {
        vectors: Vec<Vector>,
    }
    #[derive(serde::Deserialize)]
    struct Vector {
        raw: String,
        valid: bool,
        local_path: Option<String>,
        note: String,
    }

    /// The shared contract with freenet.org/open (see the module docs).
    #[test]
    fn shared_share_link_vectors() {
        let vectors: Vectors =
            serde_json::from_str(include_str!("../../../tests/data/share-link-vectors.json"))
                .expect("vector file parses");
        assert!(vectors.vectors.len() > 40, "vector file looks truncated");
        let mut valid_seen = 0;
        let mut invalid_seen = 0;
        for v in &vectors.vectors {
            let fragment = parse_share_fragment(&v.raw);
            let link = parse_freenet_link(&format!("freenet://{}", v.raw));
            assert_eq!(fragment, link, "fragment vs link disagree: {}", v.note);
            match (v.valid, fragment) {
                (true, Ok(t)) => {
                    valid_seen += 1;
                    assert_eq!(Some(t.local_path()), v.local_path, "{}", v.note);
                }
                (false, Err(_)) => invalid_seen += 1,
                (want, got) => panic!(
                    "vector {:?} ({}) expected valid={want}, got {got:?}",
                    v.raw, v.note
                ),
            }
        }
        assert!(valid_seen > 10 && invalid_seen > 20);
    }

    #[test]
    fn scheme_is_case_insensitive_and_only_freenet() {
        assert!(parse_freenet_link(&format!("FREENET://{RIVER}/")).is_ok());
        assert!(parse_freenet_link(&format!("Freenet://{RIVER}/")).is_ok());
        for bad in [
            format!("http://{RIVER}/"),
            format!("https://{RIVER}/"),
            format!("freenet:/{RIVER}/"),
            format!("freenet:///{RIVER}/"),
            format!("freenet:{RIVER}//evil/"),
            format!("freenet:{}/", RIVER.to_lowercase()),
            format!("web+freenet://{RIVER}/"),
            "javascript:alert(1)".to_string(),
            "file:///etc/passwd".to_string(),
            "freenet://".to_string(),
            String::new(),
            " freenet://x".to_string(),
        ] {
            assert!(parse_freenet_link(&bad).is_err(), "accepted {bad:?}");
        }
    }

    /// A desktop that lowercases the host must not make the authority form open
    /// a different contract. This id's lowercased form is itself a valid id.
    #[test]
    fn a_lowercased_authority_form_id_is_refused() {
        const ID: &str = "7UHpmVF4VgCDnBYRASZpmftwCn5ezSvrYQqtYYjoXZCK";
        const LOWER: &str = "7uhpmvf4vgcdnbyraszpmftwcn5ezsvryqqtyyjoxzck";
        assert!(
            parse_share_fragment(LOWER).is_ok(),
            "precondition: LOWER is a valid id"
        );
        assert!(parse_freenet_link(&format!("freenet://{ID}/")).is_ok());
        assert_eq!(
            parse_freenet_link(&format!("freenet://{LOWER}/")),
            Err(LinkError::InvalidContractId)
        );
        // The authority-less form has no host to lowercase, so it follows the
        // shared rules exactly.
        assert!(parse_freenet_link(&format!("freenet:{LOWER}/")).is_ok());
    }

    /// The authority-less form (immune to hosts being lowercased) names the
    /// same target as the authority form, and is held to the same rules.
    #[test]
    fn authority_less_form_matches_authority_form() {
        for rest in ["", "/", "/a?b=c#d", "?q", "#store=X", "/%20"] {
            assert_eq!(
                parse_freenet_link(&format!("freenet:{RIVER}{rest}")),
                parse_freenet_link(&format!("freenet://{RIVER}{rest}")),
                "{rest}"
            );
            assert!(parse_freenet_link(&format!("FREENET:{RIVER}{rest}")).is_ok());
        }
        for bad in ["/%2e%2e/x", "/../x", "/a b", "/\" --x \"y"] {
            assert!(
                parse_freenet_link(&format!("freenet:{RIVER}{bad}")).is_err(),
                "{bad}"
            );
        }
    }

    /// The output is always the fixed loopback prefix plus the validated
    /// input, whatever the input tries.
    #[test]
    fn local_url_is_fixed_loopback_prefix() {
        let t = parse_freenet_link(&format!("freenet://{RIVER}/a?b=c#d")).unwrap();
        assert_eq!(
            t.local_url(7509),
            format!("http://127.0.0.1:7509/v1/contract/web/{RIVER}/a?b=c#d")
        );
        assert_eq!(
            t.local_url(12345),
            format!("http://127.0.0.1:12345/v1/contract/web/{RIVER}/a?b=c#d")
        );
        // An '@' cannot turn the id into userinfo: it is never in the host position.
        let t = parse_freenet_link(&format!("freenet://{RIVER}/@evil.example")).unwrap();
        assert!(
            t.local_url(7509)
                .starts_with("http://127.0.0.1:7509/v1/contract/web/")
        );
        let t = parse_freenet_link(&format!("freenet://{RIVER}#@evil.example:80")).unwrap();
        assert!(
            t.local_url(7509)
                .starts_with("http://127.0.0.1:7509/v1/contract/web/")
        );
    }

    #[test]
    fn malicious_links_are_refused() {
        for bad in [
            // argument injection (Windows %1 substitution, split argv)
            format!("freenet://{RIVER}/\" --config-dir \"/tmp/x"),
            format!("freenet://{RIVER}/ --help"),
            format!("freenet://{RIVER}/\"&calc.exe"),
            format!("freenet://{RIVER}/%0d%0aHeader: x"),
            // traversal to another contract or out of the web prefix
            format!("freenet://{RIVER}/../../v1/contract/web/other/"),
            format!("freenet://{RIVER}/%2e%2e/%2e%2e/"),
            format!("freenet://{RIVER}/a/%2E%2e/"),
            format!("freenet://{RIVER}/.%2E/"),
            // authority confusion
            format!("freenet://{RIVER}//evil.example/"),
            "freenet://evil.example/".to_string(),
            "freenet://127.0.0.1:7509/".to_string(),
            format!("freenet://{}/", RIVER.to_lowercase()),
            // shell metacharacters that are outside the allowed set
            format!("freenet://{RIVER}/`id`"),
            format!("freenet://{RIVER}/a\\b"),
            format!("freenet://{RIVER}/<script>"),
        ] {
            assert!(parse_freenet_link(&bad).is_err(), "accepted {bad:?}");
        }
    }

    #[test]
    fn extra_positionals_are_refused() {
        let one = vec![OsString::from(format!("freenet://{RIVER}/"))];
        assert!(single_link(&one).is_ok());
        let split = vec![
            OsString::from(format!("freenet://{RIVER}/")),
            OsString::from("--config-dir"),
            OsString::from("/tmp/x"),
        ];
        assert!(single_link(&split).is_err());
        assert!(single_link(&[]).is_err());
    }

    /// Clap sees nothing after `--` as a flag, and a hyphenated value before
    /// it is still a positional (then refused by validation).
    #[test]
    fn clap_keeps_everything_after_double_dash_positional() {
        use clap::Parser;
        #[derive(Parser)]
        struct Cli {
            #[command(flatten)]
            open: OpenCommand,
        }
        let cli = Cli::try_parse_from(["freenet", "--", "--help"]).unwrap();
        assert_eq!(cli.open.links, vec![OsString::from("--help")]);
        let cli = Cli::try_parse_from(["freenet", "--", "a", "--version", "b"]).unwrap();
        assert_eq!(cli.open.links.len(), 3);
        assert!(single_link(&cli.open.links).is_err());
    }

    #[test]
    fn port_comes_from_config_toml() {
        assert_eq!(port_from_config_toml("ws-api-port = 7510\n"), Some(7510));
        assert_eq!(
            port_from_config_toml("mode = \"network\"\nws-api-port = 8000\n"),
            Some(8000)
        );
        assert_eq!(port_from_config_toml("mode = \"network\"\n"), None);
        assert_eq!(port_from_config_toml("ws-api-port = 0\n"), None);
        assert_eq!(port_from_config_toml("ws-api-port = 70000\n"), None);
        assert_eq!(port_from_config_toml("ws-api-port = \"7509\"\n"), None);
        assert_eq!(port_from_config_toml("not toml ["), None);

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("config.toml"), "ws-api-port = 7777\n").unwrap();
        assert_eq!(configured_port(Some(dir.path())), 7777);
        let empty = tempfile::tempdir().unwrap();
        assert_eq!(configured_port(Some(empty.path())), DEFAULT_PORT);
    }

    #[test]
    fn fallback_pages_escape_and_carry_no_link_text() {
        let page = render_fallback_page(&FallbackPage::NotRunning { port: 7509 });
        assert!(page.contains("Freenet isn't running"));
        assert!(page.contains("port 7509"));
        assert!(
            !page.contains("href=\"http://127.0.0.1"),
            "the page must not carry the link"
        );
        let invalid = render_fallback_page(&FallbackPage::InvalidLink);
        assert!(invalid.contains("isn't valid"));
        assert!(!invalid.contains("127.0.0.1"));
    }

    #[test]
    fn a_page_whose_launch_fails_is_removed() {
        let dir = tempfile::tempdir().unwrap();
        let result =
            write_and_open_fallback_page(&FallbackPage::NotRunning { port: 1 }, dir.path(), |p| {
                assert!(Path::new(p).exists(), "the page exists while being opened");
                bail!("no browser")
            });
        assert!(result.is_err());
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            0,
            "page left behind"
        );

        let mut opened = String::new();
        write_and_open_fallback_page(&FallbackPage::InvalidLink, dir.path(), |p| {
            opened = p.to_string();
            Ok(())
        })
        .unwrap();
        assert!(
            Path::new(&opened).exists(),
            "a page that opened is kept for the browser"
        );
    }

    #[test]
    fn stale_fallback_pages_are_swept_and_fresh_ones_kept() {
        let dir = tempfile::tempdir().unwrap();
        let page = dir.path().join("open-link-abc.html");
        let other = dir.path().join("unrelated.html");
        std::fs::write(&page, "x").unwrap();
        std::fs::write(&other, "x").unwrap();
        sweep_stale_fallback_pages_in(dir.path(), Duration::from_secs(3600));
        assert!(page.exists(), "a fresh page was swept");
        sweep_stale_fallback_pages_in(dir.path(), Duration::ZERO);
        assert!(!page.exists(), "a stale page was kept");
        assert!(other.exists(), "an unrelated file was swept");
    }

    #[test]
    fn a_listener_that_is_not_freenet_is_not_the_node() {
        // Something listening that does not answer like Freenet.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            use std::io::{Read, Write};
            if let Ok((mut conn, _)) = listener.accept() {
                let mut buf = [0u8; 1024];
                drop(conn.read(&mut buf));
                drop(conn.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi"));
            }
        });
        assert!(!freenet_answers_on(port));
        server.join().unwrap();
    }

    #[test]
    fn a_listener_answering_like_freenet_is_the_node() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            use std::io::{Read, Write};
            if let Ok((mut conn, _)) = listener.accept() {
                let mut buf = [0u8; 1024];
                let n = conn.read(&mut buf).unwrap_or(0);
                assert!(String::from_utf8_lossy(&buf[..n]).starts_with("GET /v1/version "));
                drop(conn.write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\r\n{\"version\":\"0.2.139\"}",
                ));
            }
        });
        assert!(freenet_answers_on(port));
        server.join().unwrap();
    }

    #[test]
    fn version_response_check() {
        let ok =
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\r\n{\"version\":\"0.2.139\"}";
        assert!(looks_like_freenet_version_response(ok));
        assert!(!looks_like_freenet_version_response(
            "HTTP/1.1 404 Not Found\r\n\r\n{\"version\":\"x\"}"
        ));
        assert!(!looks_like_freenet_version_response(
            "HTTP/1.1 200 OK\r\n\r\n<html>"
        ));
        assert!(!looks_like_freenet_version_response(
            "HTTP/1.1 200 OK\r\n\r\n{\"version\":1}"
        ));
        assert!(!looks_like_freenet_version_response("garbage"));
    }

    /// The macOS in-app handler waits for a node LaunchServices just started:
    /// polling must keep trying until the probe succeeds.
    #[test]
    fn polling_keeps_trying_until_the_node_answers() {
        let mut calls = 0;
        assert!(poll_until(
            Duration::from_secs(5),
            Duration::from_millis(1),
            || {
                calls += 1;
                calls == 3
            }
        ));
        assert_eq!(calls, 3);
        let mut calls = 0;
        assert!(!poll_until(
            Duration::ZERO,
            Duration::from_millis(1),
            || {
                calls += 1;
                false
            }
        ));
        assert_eq!(calls, 1, "at least, and with no wait exactly, one attempt");
    }
}
