//! Contracts that return out-of-range buffer or result locations must get an
//! error, never a host read/write outside their linear memory or a host panic.
//!
//! Each test builds a hand-written WAT contract from an honest template (a
//! bump allocator that lays out real `BufferBuilder`s, and exports that return
//! well-formed result descriptors) and changes exactly one guest-controlled
//! value. The honest template itself is run as the control, so a check that
//! refused honest contracts would fail here too.

use std::sync::Arc;

use freenet_stdlib::prelude::*;

use super::super::guest_mem::GuestMemoryError;
use super::super::{ContractExecError, ContractRuntimeInterface, Runtime, RuntimeInnerError};
use crate::contract::storages::Storage;
use crate::util::tests::get_temp_dir;

/// An offset far outside any instance's linear memory.
const FAR: i64 = 0x7fff_0000_0000;

/// Where each export's result descriptor and payload live in the template.
const DESCRIPTORS: [(u32, u32); 4] = [(256, 1024), (272, 2048), (288, 3072), (304, 4096)];
const VALIDATE: usize = 0;
const UPDATE: usize = 1;
const SUMMARIZE: usize = 2;
const DELTA: usize = 3;

/// One field of a result descriptor to overwrite.
#[derive(Clone, Copy)]
enum DescField {
    Ptr(i64),
    Kind(i32),
    Size(u32),
}

struct Template {
    imports: &'static str,
    /// Statements run inside `__frnt__initiate_buffer` after the honest
    /// builder at `$b` is laid out, to corrupt one of its fields.
    alloc_forge: String,
    /// The i64 `__frnt__initiate_buffer` returns.
    alloc_ret: String,
    /// Body of `validate_state(param $p i64) (param $s i64) (param $r i64)`.
    validate_body: String,
    /// The i64 each export returns as its descriptor offset.
    returns: [i64; 4],
    descriptors: [[u8; 16]; 4],
    payloads: [Vec<u8>; 4],
}

impl Template {
    fn honest() -> Self {
        let validate =
            bincode::serialize(&Ok::<ValidateResult, ContractError>(ValidateResult::Valid))
                .unwrap();
        let update = bincode::serialize(&Ok::<UpdateModification<'static>, ContractError>(
            UpdateModification::valid(State::from(vec![7, 7, 7])),
        ))
        .unwrap();
        let summary = bincode::serialize(&Ok::<StateSummary<'static>, ContractError>(
            StateSummary::from(vec![1, 2]),
        ))
        .unwrap();
        let delta = bincode::serialize(&Ok::<StateDelta<'static>, ContractError>(
            StateDelta::from(vec![3, 4]),
        ))
        .unwrap();
        let payloads = [validate, update, summary, delta];
        // stdlib's private `ResultKind` discriminants for each export.
        let kinds = [0i32, 2, 3, 4];
        let mut descriptors = [[0u8; 16]; 4];
        for i in 0..4 {
            descriptors[i][0..8].copy_from_slice(&i64::from(DESCRIPTORS[i].1).to_le_bytes());
            descriptors[i][8..12].copy_from_slice(&kinds[i].to_le_bytes());
            descriptors[i][12..16].copy_from_slice(&(payloads[i].len() as u32).to_le_bytes());
        }
        Self {
            imports: "",
            alloc_forge: String::new(),
            alloc_ret: "(i64.extend_i32_u (local.get $b))".into(),
            validate_body: format!("(i64.const {})", DESCRIPTORS[VALIDATE].0),
            returns: DESCRIPTORS.map(|(d, _)| i64::from(d)),
            descriptors,
            payloads,
        }
    }

    fn set(mut self, export: usize, field: DescField) -> Self {
        let d = &mut self.descriptors[export];
        match field {
            DescField::Ptr(v) => d[0..8].copy_from_slice(&v.to_le_bytes()),
            DescField::Kind(v) => d[8..12].copy_from_slice(&v.to_le_bytes()),
            DescField::Size(v) => d[12..16].copy_from_slice(&v.to_le_bytes()),
        }
        self
    }

    fn wasm(&self) -> Vec<u8> {
        fn data(offset: u32, bytes: &[u8]) -> String {
            let escaped: String = bytes.iter().map(|b| format!("\\{b:02x}")).collect();
            format!("(data (i32.const {offset}) \"{escaped}\")\n")
        }
        let mut segments = String::new();
        for (((desc_at, payload_at), desc), payload) in DESCRIPTORS
            .iter()
            .zip(&self.descriptors)
            .zip(&self.payloads)
        {
            segments += &data(*desc_at, desc);
            segments += &data(*payload_at, payload);
        }
        let wat = format!(
            r#"(module
  {imports}
  (memory (export "memory") 8)
  ;; Buffers are bump-allocated from 64 KiB up; descriptors live below.
  (global $next (mut i32) (i32.const 65536))
  (global $id (mut i64) (i64.const 0))
  (func (export "__frnt_set_id") (param i64) (global.set $id (local.get 0)))
  ;; An honest stdlib-shaped allocator: builder at $b, read/write positions at
  ;; $b+32/$b+40, data at $b+48.
  (func (export "__frnt__initiate_buffer") (param $cap i32) (result i64) (local $b i32)
    (local.set $b (global.get $next))
    (i64.store (local.get $b) (i64.extend_i32_u (i32.add (local.get $b) (i32.const 48))))
    (i32.store offset=8 (local.get $b) (local.get $cap))
    (i64.store offset=16 (local.get $b) (i64.extend_i32_u (i32.add (local.get $b) (i32.const 32))))
    (i64.store offset=24 (local.get $b) (i64.extend_i32_u (i32.add (local.get $b) (i32.const 40))))
    (i32.store offset=32 (local.get $b) (i32.const 0))
    (i32.store offset=40 (local.get $b) (i32.const 0))
    (global.set $next
      (i32.and (i32.add (i32.add (local.get $b) (i32.const 55)) (local.get $cap)) (i32.const -8)))
    {alloc_forge}
    {alloc_ret})
  (func (export "validate_state") (param $p i64) (param $s i64) (param $r i64) (result i64)
    {validate_body})
  (func (export "update_state") (param i64 i64 i64) (result i64) (i64.const {upd}))
  (func (export "summarize_state") (param i64 i64) (result i64) (i64.const {sum}))
  (func (export "get_state_delta") (param i64 i64 i64) (result i64) (i64.const {delta}))
  {segments})"#,
            imports = self.imports,
            alloc_forge = self.alloc_forge,
            alloc_ret = self.alloc_ret,
            validate_body = self.validate_body,
            upd = self.returns[UPDATE],
            sum = self.returns[SUMMARIZE],
            delta = self.returns[DELTA],
        );
        wat::parse_str(&wat).unwrap_or_else(|e| panic!("bad test WAT: {e}\n{wat}"))
    }
}

struct Harness {
    runtime: Runtime,
    key: ContractKey,
    _temp_dir: tempfile::TempDir,
}

async fn harness(wasm: Vec<u8>) -> Harness {
    let temp_dir = get_temp_dir();
    let db = Storage::new(temp_dir.path()).await.unwrap();
    let mut contract_store =
        super::super::ContractStore::new(temp_dir.path().join("contract"), 10_000, db.clone())
            .unwrap();
    let delegate_store =
        super::super::DelegateStore::new(temp_dir.path().join("delegate"), 10_000, db.clone())
            .unwrap();
    let secrets_store =
        super::super::SecretsStore::new(temp_dir.path().join("secrets"), Default::default(), db)
            .unwrap();
    let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
        Arc::new(ContractCode::from(wasm)),
        vec![].into(),
    )));
    let key = contract.key();
    contract_store.store_contract(contract).unwrap();
    let runtime = Runtime::build(contract_store, delegate_store, secrets_store, false).unwrap();
    Harness {
        runtime,
        key,
        _temp_dir: temp_dir,
    }
}

impl Harness {
    fn validate(&mut self, state: Vec<u8>) -> super::super::RuntimeResult<ValidateResult> {
        self.runtime.validate_state(
            &self.key,
            &Parameters::from(vec![1u8, 2, 3]),
            &WrappedState::new(state),
            &Default::default(),
        )
    }
    fn update(&mut self) -> super::super::RuntimeResult<UpdateModification<'static>> {
        self.runtime.update_state(
            &self.key,
            &Parameters::from(vec![1u8, 2, 3]),
            &WrappedState::new(vec![9; 16]),
            &[UpdateData::Delta(StateDelta::from(vec![5u8; 8]))],
        )
    }
    fn summarize(&mut self) -> super::super::RuntimeResult<StateSummary<'static>> {
        self.runtime.summarize_state(
            &self.key,
            &Parameters::from(vec![1u8, 2, 3]),
            &WrappedState::new(vec![9; 16]),
        )
    }
    fn delta(&mut self) -> super::super::RuntimeResult<StateDelta<'static>> {
        self.runtime.get_state_delta(
            &self.key,
            &Parameters::from(vec![1u8, 2, 3]),
            &WrappedState::new(vec![9; 16]),
            &StateSummary::from(vec![1u8, 2]),
        )
    }
}

/// The error must be the typed guest-memory refusal, not a panic or a
/// generic failure.
#[track_caller]
fn assert_guest_memory_error<T: std::fmt::Debug>(
    res: super::super::RuntimeResult<T>,
    check: impl FnOnce(&GuestMemoryError) -> bool,
) {
    let err = res.expect_err("forged guest value must be refused");
    if let RuntimeInnerError::ContractExecError(ContractExecError::InvalidGuestMemory(e)) =
        err.deref()
    {
        assert!(check(e), "unexpected guest memory error: {e}");
    } else {
        panic!("expected InvalidGuestMemory, got {err}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn honest_template_contract_round_trips_every_export() {
    let mut h = harness(Template::honest().wasm()).await;
    assert_eq!(h.validate(vec![1, 2, 3, 4]).unwrap(), ValidateResult::Valid);
    let upd = h.update().unwrap();
    assert_eq!(
        upd.unwrap_valid().as_ref(),
        &[7, 7, 7],
        "update_state result must decode exactly"
    );
    assert_eq!(h.summarize().unwrap().as_ref(), &[1, 2]);
    assert_eq!(h.delta().unwrap().as_ref(), &[3, 4]);
}

// ---- ANT-2026-OJZPKU6V: buffer builder returned by the guest allocator ----

#[tokio::test(flavor = "multi_thread")]
async fn allocator_returning_builder_outside_memory_is_refused() {
    for ret in [
        format!("(i64.const {FAR})"),
        "(i64.const -4096)".to_string(),
    ] {
        let mut t = Template::honest();
        t.alloc_ret = ret.clone();
        let mut h = harness(t.wasm()).await;
        assert_guest_memory_error(h.validate(vec![1, 2, 3, 4]), |e| {
            matches!(
                e,
                GuestMemoryError::OutOfBounds {
                    what: "buffer builder",
                    ..
                }
            )
        });
        // The same path serves every export.
        assert_guest_memory_error(h.summarize(), |e| {
            matches!(e, GuestMemoryError::OutOfBounds { .. })
        });
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn builder_data_start_outside_memory_is_refused() {
    let mut t = Template::honest();
    t.alloc_forge = format!("(i64.store (local.get $b) (i64.const {FAR}))");
    let mut h = harness(t.wasm()).await;
    assert_guest_memory_error(h.validate(vec![1, 2, 3, 4]), |e| {
        matches!(
            e,
            GuestMemoryError::OutOfBounds {
                what: "buffer data",
                ..
            }
        )
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn builder_capacity_past_end_of_memory_is_refused() {
    let mut t = Template::honest();
    t.alloc_forge = "(i32.store offset=8 (local.get $b) (i32.const -1))".into();
    let mut h = harness(t.wasm()).await;
    assert_guest_memory_error(h.validate(vec![1, 2, 3, 4]), |e| {
        matches!(
            e,
            GuestMemoryError::OutOfBounds {
                what: "buffer data",
                ..
            }
        )
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn builder_position_counter_outside_memory_is_refused() {
    let mut t = Template::honest();
    t.alloc_forge = format!("(i64.store offset=24 (local.get $b) (i64.const {FAR}))");
    let mut h = harness(t.wasm()).await;
    assert_guest_memory_error(h.validate(vec![1, 2, 3, 4]), |e| {
        matches!(
            e,
            GuestMemoryError::OutOfBounds {
                what: "buffer write position",
                ..
            }
        )
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn builder_write_position_past_capacity_is_refused() {
    let mut t = Template::honest();
    t.alloc_forge = "(i32.store offset=40 (local.get $b) (i32.const -1))".into();
    let mut h = harness(t.wasm()).await;
    assert_guest_memory_error(h.validate(vec![1, 2, 3, 4]), |e| {
        matches!(
            e,
            GuestMemoryError::PositionPastCapacity { what: "write", .. }
        )
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn builder_overlapping_its_own_data_is_refused() {
    let mut t = Template::honest();
    // Point the write position into the data region the host writes.
    t.alloc_forge =
        "(i64.store offset=24 (local.get $b) (i64.extend_i32_u (i32.add (local.get $b) (i32.const 48))))"
            .into();
    let mut h = harness(t.wasm()).await;
    assert_guest_memory_error(h.validate(vec![1, 2, 3, 4]), |e| {
        matches!(e, GuestMemoryError::Overlap { .. })
    });
}

/// Streaming contracts refill their input through `__frnt__fill_buffer`,
/// which rebuilds the buffer from the guest-owned builder on every call. A
/// builder the guest rewrote after the host created it must trap the guest,
/// and an honest refill must still work.
///
/// `require_data`: trap (`unreachable`) if the refill delivered nothing, which
/// proves the honest refill really wrote data. The forged cases pass `false`
/// and ignore the refill's return value, so a validate that fails can only
/// have been stopped by the host, never by the guest itself.
fn streaming_template(forge_before_fill: &str, require_data: bool) -> Template {
    let mut t = Template::honest();
    t.imports = r#"(import "freenet_contract_io" "__frnt__fill_buffer"
    (func $fill (param i64 i64) (result i32)))"#;
    let fill = "(call $fill (global.get $id) (local.get $s))";
    let call = if require_data {
        format!("(if (i32.eqz {fill}) (then unreachable))")
    } else {
        format!("(drop {fill})")
    };
    t.validate_body = format!(
        "{forge_before_fill}
    {call}
    (i64.const {})",
        DESCRIPTORS[VALIDATE].0
    );
    t
}

/// Larger than one 64 KiB streaming buffer, so a refill has data to deliver.
fn big_state() -> Vec<u8> {
    vec![0xAB; 100_000]
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_refill_of_honest_builder_still_works() {
    let mut h = harness(streaming_template("", true).wasm()).await;
    assert_eq!(h.validate(big_state()).unwrap(), ValidateResult::Valid);
}

#[tokio::test(flavor = "multi_thread")]
async fn streaming_refill_of_builder_rewritten_outside_memory_traps() {
    for forge in [
        // data start moved far outside memory
        format!("(i64.store (i32.wrap_i64 (local.get $s)) (i64.const {FAR}))"),
        // write position moved far outside memory
        format!("(i64.store offset=24 (i32.wrap_i64 (local.get $s)) (i64.const {FAR}))"),
        // capacity grown past the end of memory
        "(i32.store offset=8 (i32.wrap_i64 (local.get $s)) (i32.const -1))".to_string(),
    ] {
        let mut h = harness(streaming_template(&forge, false).wasm()).await;
        let err = h
            .validate(big_state())
            .expect_err("refill through a forged builder must fail");
        // The guest ignores the refill's result and returns a valid answer,
        // so the only way this call fails is the host trapping the refill.
        // (The trap's cause is logged by the engine; the client-facing string
        // carries only the wasm backtrace.)
        assert!(
            matches!(err.deref(), RuntimeInnerError::WasmError(_)),
            "expected the refill to trap the guest, got: {err}"
        );
    }
}

// ---- ANT-2026-ZH3GYE67: result descriptor returned by each export ----

/// A contract export may grow its memory and place its result in the new
/// pages. The host must check and read the result against the memory as it
/// is AFTER the call; checking against a view taken before the call would
/// refuse this honest result (and, if the memory moved, read a stale base).
#[tokio::test(flavor = "multi_thread")]
async fn result_placed_in_memory_grown_during_the_call_is_accepted() {
    let mut t = Template::honest();
    // $end = start of the page just grown. Trap if the grow fails, so the
    // descriptor can never fall back inside the old memory and pass vacuously.
    let end = "(i32.mul (i32.sub (memory.size) (i32.const 1)) (i32.const 65536))";
    t.validate_body = format!(
        "(if (i32.eq (memory.grow (i32.const 1)) (i32.const -1)) (then unreachable))
    ;; descriptor at $end: ptr = $end + 16, kind 0 (validate_state), size 8;
    ;; the payload (8 zero bytes = Ok(ValidateResult::Valid)) is the fresh,
    ;; zero-filled page after it.
    (i64.store {end} (i64.extend_i32_u (i32.add {end} (i32.const 16))))
    (i32.store offset=8 {end} (i32.const 0))
    (i32.store offset=12 {end} (i32.const 8))
    (i64.extend_i32_u {end})"
    );
    assert_eq!(
        bincode::serialize(&Ok::<ValidateResult, ContractError>(ValidateResult::Valid)).unwrap(),
        vec![0u8; 8],
        "the grown page's zero bytes must be the encoding of Ok(Valid)"
    );
    let mut h = harness(t.wasm()).await;
    assert_eq!(h.validate(vec![1, 2, 3, 4]).unwrap(), ValidateResult::Valid);
}

#[tokio::test(flavor = "multi_thread")]
async fn validate_state_result_with_unknown_kind_is_refused() {
    let mut h = harness(Template::honest().set(VALIDATE, DescField::Kind(7)).wasm()).await;
    assert_guest_memory_error(h.validate(vec![1, 2, 3, 4]), |e| {
        matches!(
            e,
            GuestMemoryError::WrongResultKind {
                export: "validate_state",
                expected: 0,
                found: 7
            }
        )
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn each_export_refuses_a_result_kind_of_another_export() {
    // validate_state answering with update_state's kind, and every other
    // export answering with validate_state's kind.
    let mut h = harness(
        Template::honest()
            .set(VALIDATE, DescField::Kind(2))
            .set(UPDATE, DescField::Kind(0))
            .set(SUMMARIZE, DescField::Kind(0))
            .set(DELTA, DescField::Kind(0))
            .wasm(),
    )
    .await;
    let wrong = |e: &GuestMemoryError| matches!(e, GuestMemoryError::WrongResultKind { .. });
    assert_guest_memory_error(h.validate(vec![1, 2, 3, 4]), wrong);
    assert_guest_memory_error(h.update(), wrong);
    assert_guest_memory_error(h.summarize(), wrong);
    assert_guest_memory_error(h.delta(), wrong);
}

#[tokio::test(flavor = "multi_thread")]
async fn result_payload_outside_memory_is_refused_for_every_export() {
    for field in [
        DescField::Ptr(FAR),
        DescField::Ptr(-1),
        DescField::Size(u32::MAX),
    ] {
        let mut t = Template::honest();
        for export in [VALIDATE, UPDATE, SUMMARIZE, DELTA] {
            t = t.set(export, field);
        }
        let mut h = harness(t.wasm()).await;
        let oob = |e: &GuestMemoryError| {
            matches!(
                e,
                GuestMemoryError::OutOfBounds {
                    what: "result payload",
                    ..
                }
            )
        };
        assert_guest_memory_error(h.validate(vec![1, 2, 3, 4]), oob);
        assert_guest_memory_error(h.update(), oob);
        assert_guest_memory_error(h.summarize(), oob);
        assert_guest_memory_error(h.delta(), oob);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn result_descriptor_outside_memory_is_refused_for_every_export() {
    let mut t = Template::honest();
    t.validate_body = format!("(i64.const {FAR})");
    t.returns = [FAR, FAR, -16, FAR];
    let mut h = harness(t.wasm()).await;
    let oob = |e: &GuestMemoryError| {
        matches!(
            e,
            GuestMemoryError::OutOfBounds {
                what: "result descriptor",
                ..
            }
        )
    };
    assert_guest_memory_error(h.validate(vec![1, 2, 3, 4]), oob);
    assert_guest_memory_error(h.update(), oob);
    assert_guest_memory_error(h.summarize(), oob);
    assert_guest_memory_error(h.delta(), oob);
}

// ---- delegate path: `process` input buffers ----

/// A minimal delegate: the same honest allocator, and a `process` export that
/// returns a `{ptr, size}` descriptor at 256 pointing at an empty
/// `Ok(Vec<OutboundDelegateMsg>)` payload at 1024.
fn delegate_wasm(alloc_ret: &str, payload_ptr: i64) -> Vec<u8> {
    fn data(offset: u32, bytes: &[u8]) -> String {
        let escaped: String = bytes.iter().map(|b| format!("\\{b:02x}")).collect();
        format!("(data (i32.const {offset}) \"{escaped}\")\n")
    }
    let payload =
        bincode::serialize(&Ok::<Vec<OutboundDelegateMsg>, DelegateError>(vec![])).unwrap();
    let mut desc = [0u8; 16];
    desc[0..8].copy_from_slice(&payload_ptr.to_le_bytes());
    desc[8..12].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    let wat = format!(
        r#"(module
  (memory (export "memory") 8)
  (global $next (mut i32) (i32.const 65536))
  (func (export "__frnt__initiate_buffer") (param $cap i32) (result i64) (local $b i32)
    (local.set $b (global.get $next))
    (i64.store (local.get $b) (i64.extend_i32_u (i32.add (local.get $b) (i32.const 48))))
    (i32.store offset=8 (local.get $b) (local.get $cap))
    (i64.store offset=16 (local.get $b) (i64.extend_i32_u (i32.add (local.get $b) (i32.const 32))))
    (i64.store offset=24 (local.get $b) (i64.extend_i32_u (i32.add (local.get $b) (i32.const 40))))
    (i32.store offset=32 (local.get $b) (i32.const 0))
    (i32.store offset=40 (local.get $b) (i32.const 0))
    (global.set $next
      (i32.and (i32.add (i32.add (local.get $b) (i32.const 55)) (local.get $cap)) (i32.const -8)))
    {alloc_ret})
  (func (export "process") (param i64 i64 i64) (result i64) (i64.const 256))
  {d}{p})"#,
        d = data(256, &desc),
        p = data(1024, &payload),
    );
    wat::parse_str(&wat).unwrap_or_else(|e| panic!("bad test WAT: {e}\n{wat}"))
}

async fn run_delegate(wasm: Vec<u8>) -> super::super::RuntimeResult<Vec<OutboundDelegateMsg>> {
    use super::super::DelegateRuntimeInterface;
    use chacha20poly1305::{
        XChaCha20Poly1305,
        aead::{AeadCore, KeyInit, OsRng},
    };

    let temp_dir = get_temp_dir();
    let db = Storage::new(temp_dir.path()).await.unwrap();
    let contract_store =
        super::super::ContractStore::new(temp_dir.path().join("contract"), 10_000, db.clone())
            .unwrap();
    let delegate_store =
        super::super::DelegateStore::new(temp_dir.path().join("delegate"), 10_000, db.clone())
            .unwrap();
    let secrets_store =
        super::super::SecretsStore::new(temp_dir.path().join("secrets"), Default::default(), db)
            .unwrap();
    let mut runtime = Runtime::build(contract_store, delegate_store, secrets_store, false).unwrap();
    let delegate = DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate::from((
        &wasm.into(),
        &vec![].into(),
    ))));
    runtime
        .delegate_store
        .store_delegate(delegate.clone())
        .unwrap();
    let cipher = XChaCha20Poly1305::new(&XChaCha20Poly1305::generate_key(&mut OsRng));
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    runtime
        .secret_store
        .register_delegate(delegate.key().clone(), cipher, nonce)
        .unwrap();
    runtime.inbound_app_message(
        delegate.key(),
        &Parameters::from(vec![]),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(
            ApplicationMessage::new(vec![1, 2, 3]),
        )],
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn delegate_honest_buffers_and_result_are_accepted() {
    let out = run_delegate(delegate_wasm("(i64.extend_i32_u (local.get $b))", 1024))
        .await
        .unwrap();
    assert!(out.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn delegate_allocator_returning_builder_outside_memory_is_refused() {
    let res = run_delegate(delegate_wasm(&format!("(i64.const {FAR})"), 1024)).await;
    assert_guest_memory_error(res, |e| {
        matches!(
            e,
            GuestMemoryError::OutOfBounds {
                what: "buffer builder",
                ..
            }
        )
    });
}
