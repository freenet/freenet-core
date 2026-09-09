//! Test delegate for host-side scheduled wakeups (freenet-core#3972).
//!
//! It exercises the two things nothing else can:
//!
//! 1. **The full round trip.** The delegate asks for a wakeup, the node's
//!    serial `contract_handling` loop fires it back as
//!    `InboundDelegateMsg::WakeupFired`, and the delegate records that in a
//!    SECRET — because `WakeupFired` deliberately carries no
//!    `DelegateContext`, so a delegate needing state across a wakeup must read
//!    it from its secrets. A later app message asks whether it fired.
//!
//! 2. **The host's bounds, from the position that can actually reach them.**
//!    `DelegateCtx::schedule_wakeup` refuses an oversized tag and clamps a
//!    short delay BEFORE calling the host, so a delegate using the wrapper can
//!    never observe the host's own checks. `ArmRaw` calls the import directly,
//!    which is exactly what a delegate bypassing the wrapper does — and the
//!    whole reason the host must re-check what the guest already checked.

use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};

// Declared BY HAND rather than reached through `DelegateCtx::schedule_wakeup`.
//
// This block IS the threat model: a guest-side check on a guest-declared
// import can never be a bound, because the guest declares the import. Anything
// the host does not enforce itself, it does not enforce.
#[cfg(target_family = "wasm")]
#[link(wasm_import_module = "freenet_delegate_management")]
unsafe extern "C" {
    fn __frnt__delegate__schedule_wakeup(after_millis: i64, tag_ptr: i64, tag_len: i32) -> i64;
}

#[derive(Debug, Serialize, Deserialize)]
pub enum InboundAppMessage {
    /// Ask for a wakeup through freenet-stdlib's wrapper — the well-behaved
    /// path.
    Arm { after_millis: u64, tag: Vec<u8> },
    /// Ask for one by calling the host import directly, bypassing every
    /// guest-side check. `after_millis` is `i64` so a negative delay is
    /// expressible, which the wrapper's `Duration` cannot express at all.
    ArmRaw { after_millis: i64, tag: Vec<u8> },
    /// Has the wakeup for `tag` fired yet?
    DidFire { tag: Vec<u8> },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum OutboundAppMessage {
    /// The host's answer, verbatim. `0` is success; a negative value is the
    /// refusal code, and the test asserts on the SPECIFIC one.
    Armed { code: i64 },
    /// Whether the wakeup for that tag has been delivered.
    Fired { fired: bool },
}

/// The secret a fired wakeup writes. Keyed by tag so two wakeups are
/// distinguishable.
fn fired_marker_key(tag: &[u8]) -> Vec<u8> {
    let mut key = b"wakeup-fired:".to_vec();
    key.extend_from_slice(tag);
    key
}

struct Delegate;

#[delegate]
impl DelegateInterface for Delegate {
    fn process(
        ctx: &mut DelegateCtx,
        _params: Parameters<'static>,
        _origin: Option<MessageOrigin>,
        message: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        match message {
            InboundDelegateMsg::ApplicationMessage(incoming) => {
                let request: InboundAppMessage =
                    bincode::deserialize(incoming.payload.as_slice())
                        .map_err(|err| DelegateError::Other(format!("{err}")))?;

                let response = match request {
                    InboundAppMessage::Arm { after_millis, tag } => {
                        let code = match ctx.schedule_wakeup(
                            std::time::Duration::from_millis(after_millis),
                            &tag,
                        ) {
                            Ok(()) => 0,
                            Err(code) => code,
                        };
                        OutboundAppMessage::Armed { code }
                    }
                    InboundAppMessage::ArmRaw { after_millis, tag } => {
                        OutboundAppMessage::Armed {
                            code: arm_raw(after_millis, &tag),
                        }
                    }
                    InboundAppMessage::DidFire { tag } => OutboundAppMessage::Fired {
                        fired: ctx.get_secret(&fired_marker_key(&tag)).is_some(),
                    },
                };

                let payload = bincode::serialize(&response)
                    .map_err(|err| DelegateError::Other(format!("{err}")))?;
                Ok(vec![OutboundDelegateMsg::ApplicationMessage(
                    ApplicationMessage::new(payload).processed(true),
                )])
            }
            // The wakeup arriving back. No `DelegateContext` comes with it, by
            // design — so the record goes somewhere that survives the gap
            // between invocations, which is what secrets are for.
            InboundDelegateMsg::WakeupFired { tag } => {
                ctx.set_secret(&fired_marker_key(&tag), b"fired");
                Ok(vec![])
            }
            _ => Err(DelegateError::Other(
                "unexpected inbound message".to_string(),
            )),
        }
    }
}

/// Call the host import with no guest-side validation at all.
fn arm_raw(after_millis: i64, tag: &[u8]) -> i64 {
    #[cfg(target_family = "wasm")]
    {
        unsafe {
            __frnt__delegate__schedule_wakeup(after_millis, tag.as_ptr() as i64, tag.len() as i32)
        }
    }

    #[cfg(not(target_family = "wasm"))]
    {
        let _ = (after_millis, tag);
        -99
    }
}
