//! The node binary in compile-worker mode: started with the hidden first
//! argument it must speak the worker protocol, check the handshake and exit.
//!
//! Linux only (the worker does not exist elsewhere). The binary is copied to a
//! private path first: a concurrent cargo run replaces the original, and a test
//! that executes it then fails with `NotFound`.

#![cfg(target_os = "linux")]

use std::io::{Read, Write};
use std::process::{Command, Stdio};

fn private_copy() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("fncw-bin-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let to = dir.join("freenet");
    std::fs::copy(env!("CARGO_BIN_EXE_freenet"), &to).unwrap();
    to
}

/// A request frame with an engine identity the binary cannot match.
fn request(version: u16) -> Vec<u8> {
    let mut r = Vec::new();
    r.extend_from_slice(b"FNCW");
    r.extend_from_slice(&version.to_le_bytes());
    r.extend_from_slice(&[0xAA; 8]); // engine identity
    r.extend_from_slice(&[0xBB; 16]); // build identity
    r.push(0); // flags
    r.extend_from_slice(&20_000u64.to_le_bytes()); // cpu ms
    r.extend_from_slice(&(1u64 << 30).to_le_bytes()); // data bytes
    r.extend_from_slice(&30_000u64.to_le_bytes()); // wall ms
    r.extend_from_slice(&0u32.to_le_bytes()); // module length
    r
}

/// One run at a time: copying a binary while another thread forks leaves the
/// copy open for writing in the forked child, and executing it then fails with
/// `ETXTBSY`.
static ONE_AT_A_TIME: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn run(args: &[String], stdin: &[u8]) -> (Vec<u8>, std::process::ExitStatus) {
    let _serial = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let exe = private_copy();
    let mut child = Command::new(&exe)
        .args(args)
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(stdin).unwrap();
    let mut out = Vec::new();
    child.stdout.take().unwrap().read_to_end(&mut out).unwrap();
    let st = child.wait().unwrap();
    drop(std::fs::remove_dir_all(exe.parent().unwrap()));
    (out, st)
}

fn worker_args() -> Vec<String> {
    vec![
        "__compile-worker".into(),
        "--parent-pid".into(),
        std::process::id().to_string(),
    ]
}

#[test]
fn the_binary_answers_a_request_with_a_version_mismatch_verdict() {
    let (out, st) = run(&worker_args(), &request(1));
    assert!(st.success(), "{st:?}");
    // magic, protocol version, status byte: 5 is the version-mismatch status.
    assert_eq!(&out[0..4], b"FNCW");
    assert_eq!(u16::from_le_bytes([out[4], out[5]]), 1);
    assert_eq!(
        out[6], 5,
        "an unknown engine identity is not a verdict on the module"
    );
}

#[test]
fn an_unknown_protocol_version_is_answered_the_same_way() {
    let (out, st) = run(&worker_args(), &request(99));
    assert!(st.success(), "{st:?}");
    assert_eq!(out[6], 5);
}

#[test]
fn a_worker_named_for_another_parent_exits_without_answering() {
    let args = vec![
        "__compile-worker".to_string(),
        "--parent-pid".to_string(),
        "1999999".to_string(),
    ];
    let (out, st) = run(&args, &request(1));
    assert!(out.is_empty());
    assert_eq!(st.code(), Some(3));
}

#[test]
fn without_the_parent_argument_it_refuses_to_run() {
    let (out, st) = run(&["__compile-worker".to_string()], &request(1));
    assert!(out.is_empty());
    assert_eq!(st.code(), Some(2));
}

// ---------------------------------------------------------------------------
// The production path to an accepted verdict.
// ---------------------------------------------------------------------------

/// Run the real binary as the production worker (its hidden argument, the real
/// handshake, the default caps) on `code`.
fn accepted_by_the_production_worker(code: &[u8]) -> Result<(), String> {
    let _serial = ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner());
    let exe = private_copy();
    let result = freenet::production_worker_accepts_for_tests(&exe, code);
    drop(std::fs::remove_dir_all(exe.parent().unwrap()));
    result
}

fn asset(rel: &str) -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(rel);
    std::fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// The smallest end to end check: the production binary, started as the node
/// starts it, accepts a small real contract. A mismatch between the worker's
/// engine or build identity and the node's would show here as a version-mismatch
/// answer, and in production it would silently turn isolation off at the probe.
#[test]
fn the_production_worker_accepts_a_small_real_contract() {
    let code = asset("../fdev/resources/website_contract.wasm");
    accepted_by_the_production_worker(&code).expect("accepted");
}

/// Real artifacts under the default caps: a cap change that would refuse the
/// River room contract or chat delegate fails here.
#[test]
fn the_river_room_contract_and_chat_delegate_are_accepted_under_the_default_caps() {
    for name in ["room_contract.wasm", "chat_delegate.wasm"] {
        let code = asset(&format!("tests/data/compile_worker/{name}"));
        accepted_by_the_production_worker(&code).unwrap_or_else(|e| panic!("{name}: {e}"));
    }
}
