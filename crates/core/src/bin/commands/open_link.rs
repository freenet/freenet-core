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
//!   `hugo-site/themes/freenet/layouts/shortcodes/open-link.html`). Both sides
//!   are pinned to the same vectors in `tests/data/share-link-vectors.json`, so a
//!   rule change on one side fails a test until the other side matches.
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
//! emits today) and the authority-less `freenet:<id><rest>`. The second exists
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
        Some(authority_form) => parse_share_fragment(authority_form),
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
fn default_config_dir() -> Option<PathBuf> {
    if cfg!(debug_assertions) {
        Some(std::env::temp_dir().join("freenet"))
    } else {
        directories::ProjectDirs::from("", "The Freenet Project Inc", "Freenet")
            .map(|d| d.config_dir().to_path_buf())
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

/// Whether something accepts TCP connections on `127.0.0.1:<port>`, polling
/// until `wait` has elapsed (at least one attempt is always made).
pub fn node_is_listening(port: u16, wait: Duration) -> bool {
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let deadline = Instant::now() + wait;
    loop {
        if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(1500)).is_ok() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
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
            cmd.status()
                .with_context(|| format!("failed to run {opener}"))?;
        } else {
            // Not waited on: some xdg-open backends block until the browser
            // exits. This is a short-lived process, so nothing is left behind.
            cmd.spawn()
                .with_context(|| format!("failed to run {opener}"))?;
        }
        Ok(())
    }
}

/// Escape text for an HTML attribute or element body.
fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}

/// What the local fallback page reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FallbackPage {
    /// The link was valid but no node answered on the port.
    NotRunning { local_url: String, port: u16 },
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

/// Render the fallback page. Pure, so it is unit-tested.
pub fn render_fallback_page(page: &FallbackPage) -> String {
    let (title, body) = match page {
        FallbackPage::NotRunning { local_url, port } => (
            "Freenet isn't running",
            format!(
                "<p>This link opens a Freenet site on your own computer, but no Freenet \
                 peer answered on port {port}.</p>\n<p>{}</p>\n\
                 <p><a class=\"button\" href=\"{}\">Try again</a></p>",
                start_instructions(),
                html_escape(local_url),
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
    let dir = dirs::cache_dir()
        .context("no cache directory")?
        .join("freenet");
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    let path = dir.join("open-link.html");
    write_atomically(&path, render_fallback_page(page).as_bytes())?;
    launch(&path.to_string_lossy())
}

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
    NodeNotRunning(String),
    Invalid(LinkError),
}

impl HandleOutcome {
    /// A short label for logs, carrying no part of the link.
    #[cfg_attr(not(target_os = "macos"), allow(dead_code))]
    pub fn kind(&self) -> &'static str {
        match self {
            HandleOutcome::OpenedLocal(_) => "opened",
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
        if let Err(err) = launch(&local_url) {
            tracing::warn!(error = %err, "could not open the browser");
        }
        HandleOutcome::OpenedLocal(local_url)
    } else {
        let page = FallbackPage::NotRunning {
            local_url: local_url.clone(),
            port,
        };
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
        match handle_link(link, Duration::ZERO, config_dir) {
            HandleOutcome::OpenedLocal(_) => Ok(()),
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

    /// The shared contract with freenet.org/open. The same file is run against
    /// the page's JS in freenet/web.
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
        let page = render_fallback_page(&FallbackPage::NotRunning {
            local_url: "http://127.0.0.1:7509/v1/contract/web/x/?a=1&b=\"<'".into(),
            port: 7509,
        });
        assert!(page.contains(
            "href=\"http://127.0.0.1:7509/v1/contract/web/x/?a=1&amp;b=&quot;&lt;&#39;\""
        ));
        assert!(!page.contains("b=\"<'"));
        let invalid = render_fallback_page(&FallbackPage::InvalidLink);
        assert!(invalid.contains("isn't valid"));
        assert!(!invalid.contains("127.0.0.1"));
    }

    #[test]
    fn node_listening_probe() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        assert!(node_is_listening(port, Duration::ZERO));
    }

    /// The macOS in-app handler waits for a node LaunchServices just started:
    /// a listener that appears after the first probe must still be found.
    #[test]
    fn node_listening_probe_waits_for_a_late_node() {
        // Reserve a port, release it, then bind it again from a thread after
        // a delay. (Another process taking it in between would make the
        // probe succeed early, which this test would also accept.)
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let binder = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(700));
            let listener = std::net::TcpListener::bind(("127.0.0.1", port));
            std::thread::sleep(Duration::from_secs(3));
            drop(listener);
        });
        assert!(node_is_listening(port, Duration::from_secs(5)));
        binder.join().unwrap();
    }
}
