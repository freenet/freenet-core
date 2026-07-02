//! Test delegate for the scheduled-wakeup primitive (freenet-core #3972).
//!
//! - On an [`InboundAppMessage::Schedule`] application message it emits an
//!   [`OutboundDelegateMsg::ScheduleWakeup`] for the requested absolute time and
//!   tag, plus an ack.
//! - On an [`InboundDelegateMsg::WakeupFired`] it emits an application message
//!   echoing the tag, so a test can observe that the host delivered the wakeup.

use std::time::{Duration, UNIX_EPOCH};

use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum InboundAppMessage {
    /// Ask the delegate to schedule a wakeup at `UNIX_EPOCH + (at_secs, at_nanos)`.
    Schedule {
        at_secs: u64,
        at_nanos: u32,
        tag: Vec<u8>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum OutboundAppMessage {
    /// Ack that a `ScheduleWakeup` was emitted for `tag`.
    Scheduled { tag: Vec<u8> },
    /// The delegate observed a `WakeupFired` for `tag`.
    Woke { tag: Vec<u8> },
}

struct Delegate;

#[delegate]
impl DelegateInterface for Delegate {
    fn process(
        _ctx: &mut DelegateCtx,
        _params: Parameters<'static>,
        _origin: Option<MessageOrigin>,
        message: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        match message {
            InboundDelegateMsg::ApplicationMessage(incoming) => {
                let command: InboundAppMessage = bincode::deserialize(incoming.payload.as_slice())
                    .map_err(|err| DelegateError::Other(format!("{err}")))?;
                match command {
                    InboundAppMessage::Schedule {
                        at_secs,
                        at_nanos,
                        tag,
                    } => {
                        let at = UNIX_EPOCH + Duration::new(at_secs, at_nanos);
                        let ack = bincode::serialize(&OutboundAppMessage::Scheduled {
                            tag: tag.clone(),
                        })
                        .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        Ok(vec![
                            OutboundDelegateMsg::ScheduleWakeup { at, tag },
                            OutboundDelegateMsg::ApplicationMessage(
                                ApplicationMessage::new(ack).processed(true),
                            ),
                        ])
                    }
                }
            }
            InboundDelegateMsg::WakeupFired { tag } => {
                let payload = bincode::serialize(&OutboundAppMessage::Woke { tag })
                    .map_err(|err| DelegateError::Other(format!("{err}")))?;
                Ok(vec![OutboundDelegateMsg::ApplicationMessage(
                    ApplicationMessage::new(payload).processed(true),
                )])
            }
            _ => Err(DelegateError::Other(
                "Unexpected inbound message".to_string(),
            )),
        }
    }
}
