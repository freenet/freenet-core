use std::sync::Arc;

use locutus_stdlib::prelude::*;
use serde::{Deserialize, Serialize};

const PRIVATE_KEY: [u8; 3] = [1, 2, 3];
const PUB_KEY: [u8; 1] = [1];

#[derive(Debug, Serialize, Deserialize, Default)]
struct SecretsContext {
    private_key: Option<Vec<u8>>,
}

impl SecretsContext {
    fn serialized(self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }
}

impl From<SecretsContext> for ComponentContext {
    fn from(val: SecretsContext) -> Self {
        ComponentContext(val.serialized())
    }
}

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
                    bincode::deserialize(incoming_app.payload.as_slice())
                        .map_err(|err| ComponentError::Other(format!("{err}")))?;

                match message {
                    InboundAppMessage::CreateInboxRequest => {
                        // Set secret request
                        let key = SecretsId::new(PRIVATE_KEY.to_vec());
                        let set_secret_request =
                            OutboundComponentMsg::SetSecretRequest(SetSecretRequest {
                                key,
                                value: Some(PRIVATE_KEY.to_vec()),
                            });
                        outbound.push(set_secret_request);

                        // Response with public key to the application
                        let response_msg_content: OutboundAppMessage =
                            OutboundAppMessage::CreateInboxResponse(PUB_KEY.to_vec());
                        let payload: Vec<u8> = bincode::serialize(&response_msg_content)
                            .map_err(|err| ComponentError::Other(format!("{err}")))?;
                        let response_app_msg =
                            ApplicationMessage::new(incoming_app.app, payload, true)
                                .with_context(incoming_app.context);
                        outbound.push(OutboundComponentMsg::ApplicationMessage(response_app_msg));
                    }
                    InboundAppMessage::PleaseSignMessage(inbox_priv_key) => {
                        let key = SecretsId::new(inbox_priv_key);
                        if incoming_app.context.0.is_empty() {
                            // FIXME: this msg should be added to the context so it can be signed later on
                            let request_secret = GetSecretRequest {
                                key,
                                context: SecretsContext::default().into(),
                                processed: false,
                            }
                            .into();
                            let payload = incoming_app.payload;
                            let app = incoming_app.app;
                            let please_sign_message_content =
                                ApplicationMessage::new(app, payload, false).into();
                            return Ok(vec![request_secret, please_sign_message_content]);
                        }

                        let secrets_context: SecretsContext =
                            bincode::deserialize(incoming_app.context.0.as_slice())
                                .map_err(|err| ComponentError::Other(format!("{err}")))?;

                        if let Some(_private_key) = secrets_context.private_key {
                            // Response with signature to the application
                            let signature = vec![4, 5, 2];
                            let response_msg_content: OutboundAppMessage =
                                OutboundAppMessage::MessageSigned(signature);
                            let payload: Vec<u8> = bincode::serialize(&response_msg_content)
                                .map_err(|err| ComponentError::Other(format!("{err}")))?;
                            let response_app_msg =
                                ApplicationMessage::new(incoming_app.app, payload, true)
                                    .with_context(incoming_app.context);
                            outbound
                                .push(OutboundComponentMsg::ApplicationMessage(response_app_msg));
                        } else {
                            // Secret request
                            let get_secret_request_msg =
                                OutboundComponentMsg::GetSecretRequest(GetSecretRequest {
                                    key,
                                    context: incoming_app.context.clone(),
                                    processed: false,
                                });
                            outbound.push(get_secret_request_msg);

                            // Retry sign message after secret request
                            let payload = incoming_app.payload;
                            let app = incoming_app.app;
                            let context = incoming_app.context;
                            let please_sign_message_content: ApplicationMessage =
                                ApplicationMessage::new(app, payload, false).with_context(context);
                            outbound.push(OutboundComponentMsg::ApplicationMessage(
                                please_sign_message_content,
                            ))
                        }
                    }
                }
            }
            InboundComponentMsg::GetSecretResponse(secret_response) => {
                // Response with signature to the application
                let pk_bytes = secret_response.value.unwrap();
                let serialized_context: Vec<u8> = bincode::serialize(&SecretsContext {
                    private_key: Some(pk_bytes),
                })
                .map_err(|err| ComponentError::Other(format!("{err}")))?;

                // Secret request
                let get_secret_request_msg =
                    OutboundComponentMsg::GetSecretRequest(GetSecretRequest {
                        key: secret_response.key,
                        context: ComponentContext::new(serialized_context),
                        processed: true,
                    });
                outbound.push(get_secret_request_msg);
            }
            _inbound_component_msg => {
                return Err(ComponentError::Other(
                    "Unexpected app inbound message".to_string(),
                ))
            }
        }
        Ok(outbound)
    }
}

#[test]
fn check_signing() -> Result<(), Box<dyn std::error::Error>> {
    // 1- create inbox message parts
    let contract = WrappedContract::new(
        Arc::new(ContractCode::from(vec![1])),
        Parameters::from(vec![]),
    );
    let app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();
    let payload: Vec<u8> = bincode::serialize(&InboundAppMessage::CreateInboxRequest).unwrap();
    let create_inbox_request_msg = ApplicationMessage::new(app, payload, false);

    let inbound = InboundComponentMsg::ApplicationMessage(create_inbox_request_msg);
    let output = Component::process(inbound)?;
    assert_eq!(output.len(), 2);
    assert!(matches!(
        output.first().unwrap(),
        OutboundComponentMsg::SetSecretRequest(_)
    ));
    assert!(matches!(
        output.last().unwrap(),
        OutboundComponentMsg::ApplicationMessage(_)
    ));

    // 2. request sign message
    let payload: Vec<u8> =
        bincode::serialize(&InboundAppMessage::PleaseSignMessage(PRIVATE_KEY.to_vec())).unwrap();
    let id = ContractInstanceId::try_from(['a'; 32].into_iter().collect::<String>()).unwrap();
    let sign_msg = ApplicationMessage::new(id, payload, false);
    let output = Component::process(InboundComponentMsg::ApplicationMessage(sign_msg))?;
    assert_eq!(output.len(), 2);
    assert!(matches!(
        output.first().unwrap(),
        OutboundComponentMsg::GetSecretRequest(_)
    ));
    assert!(matches!(
        output.last().unwrap(),
        OutboundComponentMsg::ApplicationMessage(_)
    ));

    // 3. sign up after getting key
    let msg = InboundComponentMsg::GetSecretResponse(GetSecretResponse {
        key: SecretsId::new(PRIVATE_KEY.to_vec()),
        value: Some(PRIVATE_KEY.to_vec()),
        context: ComponentContext::default(),
    });
    let output = Component::process(msg)?;
    assert_eq!(output.len(), 1);
    match output.first().unwrap() {
        OutboundComponentMsg::GetSecretRequest(GetSecretRequest {
            context, processed, ..
        }) if *processed => {
            let ctx: SecretsContext = bincode::deserialize(context.0.as_slice())?;
            assert!(matches!(ctx.private_key, Some(v) if v == PRIVATE_KEY));
        }
        _ => return Err("Not expected output".into()),
    };

    Ok(())
}
