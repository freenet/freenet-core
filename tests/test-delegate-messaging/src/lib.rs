use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum InboundAppMessage {
    SendToDelegate {
        target_key_bytes: Vec<u8>,
        target_code_hash: Vec<u8>,
        payload: Vec<u8>,
    },
    Ping {
        data: Vec<u8>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum OutboundAppMessage {
    MessageSent,
    DelegateMessageReceived {
        sender_key_bytes: Vec<u8>,
        payload: Vec<u8>,
    },
    PingResponse {
        data: Vec<u8>,
    },
}

struct Delegate;

#[delegate]
impl DelegateInterface for Delegate {
    fn process(
        _ctx: &mut DelegateCtx,
        _params: Parameters<'static>,
        _attested: Option<&'static [u8]>,
        messages: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        match messages {
            InboundDelegateMsg::ApplicationMessage(incoming_app) => {
                let message: InboundAppMessage =
                    bincode::deserialize(incoming_app.payload.as_slice())
                        .map_err(|err| DelegateError::Other(format!("{err}")))?;

                match message {
                    InboundAppMessage::SendToDelegate {
                        target_key_bytes,
                        target_code_hash,
                        payload,
                    } => {
                        let key_arr: [u8; 32] = target_key_bytes
                            .try_into()
                            .map_err(|_| DelegateError::Other("key must be 32 bytes".into()))?;
                        let hash_arr: [u8; 32] = target_code_hash
                            .try_into()
                            .map_err(|_| DelegateError::Other("hash must be 32 bytes".into()))?;
                        let target = DelegateKey::new(key_arr, CodeHash::new(hash_arr));

                        // Sender is a placeholder; the runtime will overwrite it
                        // with the actual sender key (sender attestation).
                        let sender = DelegateKey::new([0u8; 32], CodeHash::new([0u8; 32]));
                        let msg = DelegateMessage::new(target, sender, payload);

                        let response_payload = bincode::serialize(&OutboundAppMessage::MessageSent)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response = ApplicationMessage::new(incoming_app.app, response_payload)
                            .processed(true);

                        Ok(vec![
                            OutboundDelegateMsg::SendDelegateMessage(msg),
                            OutboundDelegateMsg::ApplicationMessage(response),
                        ])
                    }
                    InboundAppMessage::Ping { data } => {
                        let response_payload =
                            bincode::serialize(&OutboundAppMessage::PingResponse { data })
                                .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response = ApplicationMessage::new(incoming_app.app, response_payload)
                            .processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }
                }
            }
            InboundDelegateMsg::DelegateMessage(msg) => {
                let sender_key_bytes = msg.sender.bytes().to_vec();
                let response_payload =
                    bincode::serialize(&OutboundAppMessage::DelegateMessageReceived {
                        sender_key_bytes,
                        payload: msg.payload,
                    })
                    .map_err(|err| DelegateError::Other(format!("{err}")))?;

                // Use a dummy app id — in real usage the delegate would
                // track which app to notify via context.
                let app = ContractInstanceId::new([0u8; 32]);
                let response = ApplicationMessage::new(app, response_payload).processed(true);
                Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
            }
            _ => Err(DelegateError::Other(
                "Unexpected inbound message".to_string(),
            )),
        }
    }
}
