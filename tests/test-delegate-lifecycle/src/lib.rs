//! Declares a manifest asking for lifecycle events, one periodic wake-up and
//! the Background capability.
//!
//! - Each lifecycle event is answered with an `ApplicationMessage` naming the
//!   event and echoing the parameters it ran with, so a test can see both that
//!   the event decoded and that the run got the REGISTERED parameters.
//! - Each `WakeupFired` for the declared tag increments a counter kept in the
//!   delegate's secrets and records the parameters the run got. A wake-up run
//!   has no client to answer, so the secrets are the evidence; an
//!   `ApplicationMessage` `b"wakeups?"` reads them back as
//!   `"wakeups:<count>:<params>"`.

use freenet_stdlib::prelude::*;

pub struct LifecycleDelegate;

const WAKEUP_TAG: &[u8] = b"heartbeat";
const COUNT_KEY: &[u8] = b"wakeup_count";
const PARAMS_KEY: &[u8] = b"wakeup_params";

#[delegate(manifest(
    lifecycle = [Installed, NodeStarted],
    capabilities = [Background],
    wakeups = [heartbeat = 60]
))]
impl DelegateInterface for LifecycleDelegate {
    fn process(
        ctx: &mut DelegateCtx,
        parameters: Parameters<'static>,
        _origin: Option<MessageOrigin>,
        message: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        let name: &[u8] = match message {
            InboundDelegateMsg::Lifecycle(LifecycleEvent::Installed) => b"installed",
            InboundDelegateMsg::Lifecycle(LifecycleEvent::NodeStarted { .. }) => b"node_started",
            InboundDelegateMsg::WakeupFired { tag } => {
                if tag == WAKEUP_TAG {
                    let count = ctx
                        .get_secret(COUNT_KEY)
                        .and_then(|b| b.try_into().ok())
                        .map(u32::from_le_bytes)
                        .unwrap_or(0);
                    ctx.set_secret(COUNT_KEY, &(count + 1).to_le_bytes());
                    ctx.set_secret(PARAMS_KEY, parameters.as_ref());
                }
                return Ok(vec![]);
            }
            InboundDelegateMsg::ApplicationMessage(msg) if msg.payload == b"wakeups?" => {
                let count = ctx
                    .get_secret(COUNT_KEY)
                    .and_then(|b| b.try_into().ok())
                    .map(u32::from_le_bytes)
                    .unwrap_or(0);
                let mut payload = format!("wakeups:{count}:").into_bytes();
                payload.extend(ctx.get_secret(PARAMS_KEY).unwrap_or_default());
                return Ok(vec![OutboundDelegateMsg::ApplicationMessage(
                    ApplicationMessage::new(payload),
                )]);
            }
            InboundDelegateMsg::ApplicationMessage(_) => b"app",
            _ => return Ok(vec![]),
        };
        let mut payload = name.to_vec();
        payload.push(b':');
        payload.extend_from_slice(parameters.as_ref());
        Ok(vec![OutboundDelegateMsg::ApplicationMessage(
            ApplicationMessage::new(payload),
        )])
    }
}
