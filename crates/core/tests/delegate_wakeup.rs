//! End-to-end coverage for host-side scheduled delegate wakeups
//! (freenet-core#3972), against a real node and a real WASM delegate.
//!
//! What only this test can reach:
//!
//! * **The round trip.** The unit tests drive the broker directly and the
//!   instantiation tests prove the import resolves. Neither shows a delegate
//!   asking for a wakeup and being re-entered with `WakeupFired` by the serial
//!   `contract_handling` loop, which is the whole feature.
//!
//! * **The refusal codes as a delegate actually sees them.** A code is only
//!   worth having if it survives the trip from
//!   `delegate_wakeups::WakeupRefusal` through the host function, the WASM
//!   boundary and the guest's `Result<(), i64>` intact. Asserting on the
//!   constants in-process does not show that.
//!
//! * **The bypass path.** `DelegateCtx::schedule_wakeup` refuses an oversized
//!   tag and clamps a short delay before calling the host, so the wrapper can
//!   never produce the host's own refusals. The fixture's `ArmRaw` declares the
//!   import by hand — the position from which a guest-side check is not a bound
//!   — and that is the only way to observe that the host re-checks.

use anyhow::{bail, ensure};
use freenet::test_utils::{TestContext, TestResult, load_delegate};
use freenet_macros::freenet_test;
use freenet_stdlib::{
    client_api::{ClientRequest, HostResponse, WebApi},
    prelude::*,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::connect_async;

/// Must stay in sync with `tests/test-delegate-wakeup/src/lib.rs`.
#[derive(Debug, Serialize, Deserialize)]
enum InboundAppMessage {
    Arm { after_millis: u64, tag: Vec<u8> },
    ArmRaw { after_millis: i64, tag: Vec<u8> },
    DidFire { tag: Vec<u8> },
}

/// Must stay in sync with `tests/test-delegate-wakeup/src/lib.rs`.
#[derive(Debug, Serialize, Deserialize)]
enum OutboundAppMessage {
    Armed { code: i64 },
    Fired { fired: bool },
}

const TEST_DELEGATE_CIPHER: [u8; 32] = [
    0xa1, 0x9c, 0x42, 0x7e, 0x55, 0x3d, 0xe1, 0x08, 0xb4, 0xc7, 0x77, 0x21, 0x1f, 0x09, 0xd5, 0x6a,
    0x4e, 0x83, 0xee, 0x12, 0x6d, 0xaa, 0x90, 0x35, 0x88, 0x14, 0xc2, 0xfb, 0x29, 0x47, 0x6c, 0xb0,
];
const TEST_DELEGATE_NONCE: [u8; 24] = [0u8; 24];

/// The host codes, restated here rather than imported.
///
/// A delegate author reads them out of the documentation, not out of core's
/// source, so this test stands where they do: if a constant is renumbered, this
/// fails, which is the point. Importing them would make the assertion a
/// tautology.
const ERR_WAKEUP_TAG_TOO_LONG: i64 = -40;
const ERR_WAKEUP_DELAY_TOO_SHORT: i64 = -41;
const ERR_WAKEUP_DELAY_TOO_LONG: i64 = -42;
const ERR_WAKEUP_DELEGATE_FULL: i64 = -43;

/// freenet-stdlib's `MAX_WAKEUP_TAG_BYTES`.
const MAX_WAKEUP_TAG_BYTES: usize = 128;
/// `delegate_wakeups::MAX_WAKEUPS_PER_DELEGATE`.
const MAX_WAKEUPS_PER_DELEGATE: usize = 16;
/// `delegate_wakeups::MAX_WAKEUP_DELAY`, in milliseconds.
const MAX_WAKEUP_DELAY_MILLIS: i64 = 30 * 24 * 60 * 60 * 1000;

async fn ask(
    client: &mut WebApi,
    delegate_key: &DelegateKey,
    request: InboundAppMessage,
) -> anyhow::Result<OutboundAppMessage> {
    let payload = bincode::serialize(&request)?;
    client
        .send(ClientRequest::DelegateOp(
            freenet_stdlib::client_api::DelegateRequest::ApplicationMessages {
                key: delegate_key.clone(),
                params: Parameters::from(vec![]),
                inbound: vec![InboundDelegateMsg::ApplicationMessage(
                    ApplicationMessage::new(payload),
                )],
            },
        ))
        .await?;

    let resp = timeout(Duration::from_secs(60), client.recv()).await??;
    match resp {
        HostResponse::DelegateResponse { key, values } => {
            ensure!(&key == delegate_key, "delegate key mismatch in response");
            values
                .iter()
                .find_map(|v| {
                    if let OutboundDelegateMsg::ApplicationMessage(msg) = v {
                        bincode::deserialize::<OutboundAppMessage>(&msg.payload).ok()
                    } else {
                        None
                    }
                })
                .ok_or_else(|| anyhow::anyhow!("no ApplicationMessage in {request:?} response"))
        }
        other => bail!("unexpected response to {request:?}: {other:?}"),
    }
}

fn armed_code(response: OutboundAppMessage) -> anyhow::Result<i64> {
    match response {
        OutboundAppMessage::Armed { code } => Ok(code),
        other => bail!("expected Armed, got {other:?}"),
    }
}

#[freenet_test(
    health_check_readiness = true,
    nodes = ["gateway"],
    timeout_secs = 300,
    startup_wait_secs = 20,
    tokio_flavor = "multi_thread",
    tokio_worker_threads = 4
)]
async fn test_delegate_wakeup_fires_and_refusals_reach_the_delegate(
    ctx: &mut TestContext,
) -> TestResult {
    const TEST_DELEGATE: &str = "test-delegate-wakeup";

    let delegate = load_delegate(TEST_DELEGATE, Parameters::from(vec![]))?;
    let delegate_key = delegate.key().clone();

    let gateway = ctx.node("gateway")?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let (stream, _) = connect_async(&gateway.ws_url()).await?;
    let mut client = WebApi::start(stream);

    client
        .send(ClientRequest::DelegateOp(
            freenet_stdlib::client_api::DelegateRequest::RegisterDelegate {
                delegate: delegate.clone(),
                cipher: TEST_DELEGATE_CIPHER,
                nonce: TEST_DELEGATE_NONCE,
            },
        ))
        .await?;
    match timeout(Duration::from_secs(30), client.recv()).await?? {
        HostResponse::DelegateResponse { key, .. } => {
            ensure!(key == delegate_key, "delegate key mismatch on register")
        }
        other => bail!("unexpected register response: {other:?}"),
    }

    // ------------------------------------------------------------------
    // 1. The round trip: ask, wait, and be re-entered.
    // ------------------------------------------------------------------

    let code = armed_code(
        ask(
            &mut client,
            &delegate_key,
            InboundAppMessage::Arm {
                after_millis: 1_000,
                tag: b"tick".to_vec(),
            },
        )
        .await?,
    )?;
    ensure!(
        code == 0,
        "a well-formed wakeup must be granted, got {code}"
    );

    // Asked BEFORE the deadline, so a `true` here would mean the marker was
    // written by something other than the wakeup — the assertion below would
    // otherwise pass against a delegate that simply always says yes.
    match ask(
        &mut client,
        &delegate_key,
        InboundAppMessage::DidFire {
            tag: b"tick".to_vec(),
        },
    )
    .await?
    {
        OutboundAppMessage::Fired { fired } => {
            ensure!(!fired, "the wakeup reported firing before its deadline")
        }
        other => bail!("expected Fired, got {other:?}"),
    }

    // The node fires it from the `contract_handling` loop's own deadline arm,
    // with no other traffic required — an idle node is precisely the case a
    // scheduled wakeup exists for.
    tokio::time::sleep(Duration::from_secs(5)).await;

    match ask(
        &mut client,
        &delegate_key,
        InboundAppMessage::DidFire {
            tag: b"tick".to_vec(),
        },
    )
    .await?
    {
        OutboundAppMessage::Fired { fired } => ensure!(
            fired,
            "the wakeup never came back as WakeupFired; the delegate recorded nothing"
        ),
        other => bail!("expected Fired, got {other:?}"),
    }

    // A tag nobody scheduled must NOT report as fired, or the marker is not
    // per-tag and the assertion above proves less than it looks.
    match ask(
        &mut client,
        &delegate_key,
        InboundAppMessage::DidFire {
            tag: b"never-armed".to_vec(),
        },
    )
    .await?
    {
        OutboundAppMessage::Fired { fired } => {
            ensure!(!fired, "an unarmed tag reported as fired")
        }
        other => bail!("expected Fired, got {other:?}"),
    }

    // ------------------------------------------------------------------
    // 2. Each refusal reaches the delegate as its OWN code.
    //
    // Every one of these goes through `ArmRaw`, which calls the host import
    // directly: `DelegateCtx::schedule_wakeup` refuses an oversized tag and
    // clamps a short delay before the host ever sees them, so none of these
    // are reachable through the wrapper. That is exactly why the host must
    // check them itself.
    // ------------------------------------------------------------------

    let code = armed_code(
        ask(
            &mut client,
            &delegate_key,
            InboundAppMessage::ArmRaw {
                after_millis: 0,
                tag: b"too-soon".to_vec(),
            },
        )
        .await?,
    )?;
    ensure!(
        code == ERR_WAKEUP_DELAY_TOO_SHORT,
        "a sub-second delay must be refused with its own code, got {code}"
    );

    let code = armed_code(
        ask(
            &mut client,
            &delegate_key,
            InboundAppMessage::ArmRaw {
                after_millis: MAX_WAKEUP_DELAY_MILLIS + 1,
                tag: b"too-far".to_vec(),
            },
        )
        .await?,
    )?;
    ensure!(
        code == ERR_WAKEUP_DELAY_TOO_LONG,
        "a delay past the horizon must be refused with its own code, got {code}"
    );

    let code = armed_code(
        ask(
            &mut client,
            &delegate_key,
            InboundAppMessage::ArmRaw {
                after_millis: 60_000,
                tag: vec![b'x'; MAX_WAKEUP_TAG_BYTES + 1],
            },
        )
        .await?,
    )?;
    ensure!(
        code == ERR_WAKEUP_TAG_TOO_LONG,
        "an oversized tag must be refused with its own code, got {code}"
    );

    // The boundary, so the cap is shown not to be one byte tight.
    let code = armed_code(
        ask(
            &mut client,
            &delegate_key,
            InboundAppMessage::ArmRaw {
                after_millis: 3_600_000,
                tag: vec![b'x'; MAX_WAKEUP_TAG_BYTES],
            },
        )
        .await?,
    )?;
    ensure!(
        code == 0,
        "a tag exactly at the cap must be accepted, got {code}"
    );

    // ------------------------------------------------------------------
    // 3. The per-delegate lease cap, from the delegate's side.
    //
    // One lease is already held (the 128-byte tag above), and "tick" has
    // fired and released its own. Fill the rest, then ask for one more.
    // ------------------------------------------------------------------

    for i in 0..MAX_WAKEUPS_PER_DELEGATE {
        let code = armed_code(
            ask(
                &mut client,
                &delegate_key,
                InboundAppMessage::Arm {
                    after_millis: 3_600_000,
                    tag: format!("fill-{i}").into_bytes(),
                },
            )
            .await?,
        )?;
        if code == ERR_WAKEUP_DELEGATE_FULL {
            // Reached the cap early because of the leases already held. That
            // is the assertion, so stop here.
            return Ok(());
        }
        ensure!(code == 0, "unexpected refusal while filling: {code}");
    }

    let code = armed_code(
        ask(
            &mut client,
            &delegate_key,
            InboundAppMessage::Arm {
                after_millis: 3_600_000,
                tag: b"one-too-many".to_vec(),
            },
        )
        .await?,
    )?;
    ensure!(
        code == ERR_WAKEUP_DELEGATE_FULL,
        "a delegate over its lease cap must be told so specifically, got {code}"
    );

    Ok(())
}
