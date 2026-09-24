//! Declares a manifest asking for both lifecycle events and the Background
//! capability, and answers each lifecycle event with an `ApplicationMessage`
//! naming the event and echoing the parameters it ran with, so a test can see
//! both that the event decoded and that the run got the REGISTERED parameters.

use freenet_stdlib::prelude::*;

pub struct LifecycleDelegate;

#[delegate(manifest(lifecycle = [Installed, NodeStarted], capabilities = [Background]))]
impl DelegateInterface for LifecycleDelegate {
    fn process(
        _ctx: &mut DelegateCtx,
        parameters: Parameters<'static>,
        _origin: Option<MessageOrigin>,
        message: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        let name: &[u8] = match message {
            InboundDelegateMsg::Lifecycle(LifecycleEvent::Installed) => b"installed",
            InboundDelegateMsg::Lifecycle(LifecycleEvent::NodeStarted { .. }) => b"node_started",
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
