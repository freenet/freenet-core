//! Reports the parameters each run was given, so a test can compare a run a
//! CLIENT starts (which supplies the parameters itself) with one the NODE
//! starts on a contract notification (which must look them up), #5616.
//!
//! - A `ContractNotification` records the parameters of that run in the
//!   delegate's secrets. A notification run has no client to answer, so the
//!   secret is the evidence.
//! - An `ApplicationMessage` `b"params?"` answers
//!   `"client:<params>|notified:<recorded>"`, or `"...|notified:none"` before
//!   any notification has run.

use freenet_stdlib::prelude::*;

pub struct EchoParamsDelegate;

const NOTIFIED_KEY: &[u8] = b"notified_params";

#[delegate]
impl DelegateInterface for EchoParamsDelegate {
    fn process(
        ctx: &mut DelegateCtx,
        parameters: Parameters<'static>,
        _origin: Option<MessageOrigin>,
        message: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        match message {
            InboundDelegateMsg::ContractNotification(_) => {
                ctx.set_secret(NOTIFIED_KEY, parameters.as_ref());
                Ok(vec![])
            }
            InboundDelegateMsg::ApplicationMessage(msg) if msg.payload == b"params?" => {
                let mut payload = b"client:".to_vec();
                payload.extend_from_slice(parameters.as_ref());
                payload.extend_from_slice(b"|notified:");
                match ctx.get_secret(NOTIFIED_KEY) {
                    Some(recorded) => payload.extend(recorded),
                    None => payload.extend_from_slice(b"none"),
                }
                Ok(vec![OutboundDelegateMsg::ApplicationMessage(
                    ApplicationMessage::new(payload),
                )])
            }
            _ => Ok(vec![]),
        }
    }
}
