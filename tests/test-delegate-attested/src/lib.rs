use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};

/// Sent by the test to trigger an attested-bytes echo.
#[derive(Debug, Serialize, Deserialize)]
pub enum InboundAppMessage {
    CheckAttested,
}

/// Returned by the delegate: the raw bytes that were in the `attested` parameter,
/// or `None` when the connection had no auth token.
#[derive(Debug, Serialize, Deserialize)]
pub enum OutboundAppMessage {
    Attested(Option<Vec<u8>>),
}

struct Delegate;

#[delegate]
impl DelegateInterface for Delegate {
    fn process(
        _ctx: &mut DelegateCtx,
        _params: Parameters<'static>,
        origin: Option<MessageOrigin>,
        messages: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        match messages {
            InboundDelegateMsg::ApplicationMessage(incoming) => {
                let _: InboundAppMessage = bincode::deserialize(incoming.payload.as_slice())
                    .map_err(|e| DelegateError::Other(format!("deserialize inbound: {e}")))?;

                let response =
                    OutboundAppMessage::Attested(origin.map(|o| bincode::serialize(&o).unwrap()));
                let payload = bincode::serialize(&response)
                    .map_err(|e| DelegateError::Other(format!("serialize response: {e}")))?;
                let app_msg = ApplicationMessage::new(payload).processed(true);
                Ok(vec![OutboundDelegateMsg::ApplicationMessage(app_msg)])
            }
            _ => Err(DelegateError::Other("unsupported message type".into())),
        }
    }
}

#[test]
fn test_delegate_attested_unit() -> Result<(), Box<dyn std::error::Error>> {
    use std::sync::Arc;

    let contract = WrappedContract::new(
        Arc::new(ContractCode::from(vec![1])),
        Parameters::from(vec![]),
    );
    let app_id = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

    // Test with Some origin (WebApp)
    let origin = MessageOrigin::WebApp(app_id);
    let payload = bincode::serialize(&InboundAppMessage::CheckAttested).unwrap();
    let msg = ApplicationMessage::new(payload);
    let inbound = InboundDelegateMsg::ApplicationMessage(msg);
    let mut ctx = DelegateCtx::default();
    let output = Delegate::process(
        &mut ctx,
        Parameters::from(vec![]),
        Some(origin.clone()),
        inbound,
    )?;

    assert_eq!(output.len(), 1);
    let app_msg = match output.first().unwrap() {
        OutboundDelegateMsg::ApplicationMessage(m) => m,
        _ => panic!("Expected ApplicationMessage"),
    };
    assert!(app_msg.processed);
    let resp: OutboundAppMessage = bincode::deserialize(&app_msg.payload)?;
    match resp {
        OutboundAppMessage::Attested(Some(bytes)) => {
            let deserialized: MessageOrigin = bincode::deserialize(&bytes).unwrap();
            assert_eq!(deserialized, origin);
        }
        other => panic!("Expected Attested(Some(..)), got {:?}", other),
    }

    // Test with None (no auth token)
    let payload2 = bincode::serialize(&InboundAppMessage::CheckAttested).unwrap();
    let msg2 = ApplicationMessage::new(payload2);
    let inbound2 = InboundDelegateMsg::ApplicationMessage(msg2);
    let mut ctx2 = DelegateCtx::default();
    let output2 = Delegate::process(&mut ctx2, Parameters::from(vec![]), None, inbound2)?;

    let app_msg2 = match output2.first().unwrap() {
        OutboundDelegateMsg::ApplicationMessage(m) => m,
        _ => panic!("Expected ApplicationMessage"),
    };
    let resp2: OutboundAppMessage = bincode::deserialize(&app_msg2.payload)?;
    assert!(matches!(resp2, OutboundAppMessage::Attested(None)));

    Ok(())
}
