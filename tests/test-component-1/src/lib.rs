use locutus_stdlib::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
enum InboundAppMessage {
    CreateInboxRequest,
    PleaseSignMessage(Vec<u8>),
}

#[derive(Debug, Serialize, Deserialize)]
enum OutboundAppMessage {
    CreateInboxResponse(Vec<u8>),
    MessageSigned(Vec<u8>),
}

struct Component;

#[component]
impl ComponentInterface for Component {
    fn process(messages: InboundComponentMsg) -> Result<Vec<OutboundComponentMsg>, ComponentError> {
        let mut outbound = Vec::new();
        match messages {
            InboundComponentMsg::ApplicationMessage(incoming_app) => {
                let message: InboundAppMessage =
                    serde_json::from_slice::<InboundAppMessage>(incoming_app.payload.as_slice())
                        .map_err(|err| ComponentError::Other(format!("{err}")))?;

                match message {
                    InboundAppMessage::CreateInboxRequest => {
                        // Set secret request
                        let private_key = vec![1, 2, 3];
                        let public_key = vec![1];
                        let key = SecretsId::new(private_key.clone());
                        let secret_request =
                            OutboundComponentMsg::SetSecretRequest(SetSecretRequest {
                                key,
                                value: Some(private_key),
                            });
                        outbound.push(secret_request);

                        // Response with public key to the application
                        let response_msg_content: OutboundAppMessage =
                            OutboundAppMessage::CreateInboxResponse(public_key);
                        let payload: Vec<u8> = serde_json::to_vec(&response_msg_content)
                            .map_err(|err| ComponentError::Other(format!("{err}")))?;
                        let response_app_msg = ApplicationMessage::new(
                            incoming_app.app,
                            payload,
                            incoming_app.context,
                        );
                        outbound.push(OutboundComponentMsg::ApplicationMessage(response_app_msg));
                    }
                    InboundAppMessage::PleaseSignMessage(_) => {
                        return Err(ComponentError::Other(format!(
                            "Unexpected app inbound message content"
                        )))
                    }
                }
            }
            _InboundComponentMsg => {
                return Err(ComponentError::Other(format!(
                    "Unexpected app inbound message"
                )))
            }
        }
        Ok(outbound)
    }
}
