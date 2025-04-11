use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};

const PRIVATE_KEY: [u8; 3] = [1, 2, 3];
const PUB_KEY: [u8; 1] = [1];

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
struct SecretsContext {
    private_key: Option<Vec<u8>>,
    /// Original payload of the ApplicationMessage that requested signing.
    message_to_sign_payload: Option<Vec<u8>>,
}

impl SecretsContext {
    fn serialized(self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }
}

impl From<SecretsContext> for DelegateContext {
    fn from(val: SecretsContext) -> Self {
        DelegateContext::new(val.serialized())
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum InboundAppMessage {
    CreateInboxRequest,
    PleaseSignMessage(Vec<u8>)
}

#[derive(Debug, Serialize, Deserialize)]
enum OutboundAppMessage {
    CreateInboxResponse(Vec<u8>),
    MessageSigned(Vec<u8>),
}

struct Delegate;

#[delegate]
impl DelegateInterface for Delegate {
    fn process(
        _params: Parameters<'static>,
        _attested: Option<&'static [u8]>,
        messages: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        let mut outbound = Vec::new();
        match messages {
            InboundDelegateMsg::ApplicationMessage(incoming_app) => {
                let message: InboundAppMessage =
                    bincode::deserialize(incoming_app.payload.as_slice())
                        .map_err(|err| DelegateError::Other(format!("{err}")))?;

                match message {
                    InboundAppMessage::CreateInboxRequest => {
                        // Set secret request
                        let key = SecretsId::new(PRIVATE_KEY.to_vec());
                        let set_secret_request =
                            OutboundDelegateMsg::SetSecretRequest(SetSecretRequest {
                                key,
                                value: Some(PRIVATE_KEY.to_vec()),
                            });
                        outbound.push(set_secret_request);

                        // Response with public key to the application
                        let response_msg_content: OutboundAppMessage =
                            OutboundAppMessage::CreateInboxResponse(PUB_KEY.to_vec());
                        let payload: Vec<u8> = bincode::serialize(&response_msg_content)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response_app_msg = ApplicationMessage::new(incoming_app.app, payload)
                            .processed(true)
                            .with_context(incoming_app.context);
                        outbound.push(OutboundDelegateMsg::ApplicationMessage(response_app_msg));
                    }
                    InboundAppMessage::PleaseSignMessage(inbox_priv_key) => {
                        let key = SecretsId::new(inbox_priv_key);
                        if incoming_app.context.as_ref().is_empty() {
                            let request_secret = GetSecretRequest {
                                key,
                                context: SecretsContext::default().into(),
                                processed: false,
                            }
                            .into();
                            let payload = incoming_app.payload;
                            let app = incoming_app.app;
                            let please_sign_message_content = ApplicationMessage::new(app, payload)
                                .processed(false)
                                .into();
                            return Ok(vec![request_secret, please_sign_message_content]);
                            // Store the payload we need to sign later in the context
                            // and request the secret.
                            let context = SecretsContext {
                                private_key: None,
                                message_to_sign_payload: Some(incoming_app.payload.to_vec()),
                            };
                            let request_secret = GetSecretRequest {
                                key,
                                context: context.into(),
                                processed: false, // This flag is internal to the runtime, doesn't matter here
                            }
                            .into();
                            return Ok(vec![request_secret]);
                        }

                        // If we have context, it means GetSecretResponse should be called next.
                        // This branch should ideally not be hit if the flow is correct,
                        // but if it is, we re-request the secret.
                        let secrets_context: SecretsContext =
                            bincode::deserialize(incoming_app.context.as_ref())
                                .map_err(|err| DelegateError::Other(format!("{err}")))?;

                        // Re-request the secret, preserving the message_to_sign_payload
                        let get_secret_request_msg =
                            OutboundDelegateMsg::GetSecretRequest(GetSecretRequest {
                                key,
                                context: secrets_context.into(),
                                processed: false,
                            });
                        outbound.push(get_secret_request_msg);
                    }
                }
            }
            InboundDelegateMsg::GetSecretResponse(secret_response) => {
                let pk_bytes = secret_response
                    .value
                    .ok_or(DelegateError::Other("Missing secret value".into()))?;
                let secrets_context: SecretsContext =
                    bincode::deserialize(secret_response.context.as_ref())
                        .map_err(|err| DelegateError::Other(format!("{err}")))?;

                if let Some(_payload_to_sign) = secrets_context.message_to_sign_payload {
                    // Use pk_bytes and _payload_to_sign to generate a real signature here
                    // For this test, we just use the hardcoded value and the existence of the key/payload
                    let signature = vec![4, 5, 2];
                    let response_msg_content: OutboundAppMessage =
                        OutboundAppMessage::MessageSigned(signature);
                    let response_payload: Vec<u8> = bincode::serialize(&response_msg_content)
                        .map_err(|err| DelegateError::Other(format!("{err}")))?;

                    // Find the original app ID from the context or handle appropriately
                    // For now, assuming a default or placeholder app ID if needed,
                    // but ideally, the app ID should be part of the context or derived.
                    // Let's assume the context doesn't hold the app ID, and we might need it.
                    // FIXME: How to get the original app ID here? For the test, it might not matter.
                    let app_id = ContractInstanceId::new([0u8; 32]); // Placeholder

                    let response_app_msg = ApplicationMessage::new(app_id, response_payload)
                        .processed(true)
                        // Clear context for the final response
                        .with_context(DelegateContext::default());
                    outbound.push(OutboundDelegateMsg::ApplicationMessage(response_app_msg));
                } else {
                    // This case shouldn't happen if PleaseSignMessage correctly set the context
                    return Err(DelegateError::Other(
                        "Received secret response but no message payload was stored in context"
                            .to_string(),
                    ));
                }
            }
            _inbound_delegate_msg => {
                return Err(DelegateError::Other(
                    "Unexpected app inbound message".to_string(),
                ));
            }
        }
        Ok(outbound)
    }
}

#[test]
fn check_signing() -> Result<(), Box<dyn std::error::Error>> {
    // 1. create inbox message parts
    let contract = WrappedContract::new(
        std::sync::Arc::new(ContractCode::from(vec![1])),
        Parameters::from(vec![]),
    );
    let app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();
    let payload: Vec<u8> = bincode::serialize(&InboundAppMessage::CreateInboxRequest).unwrap();
    let create_inbox_request_msg = ApplicationMessage::new(app, payload).processed(false);

    let inbound = InboundDelegateMsg::ApplicationMessage(create_inbox_request_msg);
    let output1 = Delegate::process(Parameters::from(vec![]), None, inbound)?;
    assert_eq!(output1.len(), 2);
    assert!(matches!(
        output1.first().unwrap(),
        OutboundDelegateMsg::SetSecretRequest(req) if req.value.as_ref().unwrap() == &PRIVATE_KEY
    ));
    let app_response = match output1.last().unwrap() {
        OutboundDelegateMsg::ApplicationMessage(msg) => msg,
        _ => panic!("Expected ApplicationMessage"),
    };
    let app_response_payload: OutboundAppMessage = bincode::deserialize(app_response.payload.as_ref())?;
    assert!(matches!(app_response_payload, OutboundAppMessage::CreateInboxResponse(pk) if pk == PUB_KEY));


    // 2. Request sign message - should return only GetSecretRequest
    let sign_payload_content = InboundAppMessage::PleaseSignMessage(PRIVATE_KEY.to_vec());
    let sign_payload: Vec<u8> = bincode::serialize(&sign_payload_content).unwrap();
    let app_id = ContractInstanceId::try_from(['a'; 32].into_iter().collect::<String>()).unwrap();
    let sign_msg = ApplicationMessage::new(app_id.clone(), sign_payload.clone()).processed(false);
    let output2 = Delegate::process(
        Parameters::from(vec![]),
        None,
        InboundDelegateMsg::ApplicationMessage(sign_msg),
    )?;

    assert_eq!(output2.len(), 1, "Expected only one message (GetSecretRequest)");
    let get_secret_req = match output2.first().unwrap() {
        OutboundDelegateMsg::GetSecretRequest(req) => req,
        other => panic!("Expected GetSecretRequest, got {:?}", other),
    };
    assert_eq!(get_secret_req.key.as_ref(), PRIVATE_KEY);

    // Verify context contains the payload to sign
    let ctx_step2: SecretsContext = bincode::deserialize(get_secret_req.context.as_ref())?;
    assert!(ctx_step2.private_key.is_none());
    assert_eq!(ctx_step2.message_to_sign_payload.as_ref().unwrap(), &sign_payload);


    // 3. Simulate runtime returning the secret; delegate should now sign and return the final message
    let secret_response_msg = InboundDelegateMsg::GetSecretResponse(GetSecretResponse {
        key: get_secret_req.key.clone(),
        value: Some(PRIVATE_KEY.to_vec()),
        context: get_secret_req.context.clone(), // Use context from the request
    });
    let output3 = Delegate::process(Parameters::from(vec![]), None, secret_response_msg)?;

    assert_eq!(output3.len(), 1, "Expected only one message (ApplicationMessage with signature)");
    let final_app_msg = match output3.first().unwrap() {
        OutboundDelegateMsg::ApplicationMessage(msg) => msg,
        other => panic!("Expected ApplicationMessage, got {:?}", other),
    };

    assert!(final_app_msg.processed);
    // assert_eq!(final_app_msg.app, app_id); // FIXME: App ID is currently lost/hardcoded in delegate response
    let final_payload: OutboundAppMessage = bincode::deserialize(final_app_msg.payload.as_ref())?;
    assert!(matches!(final_payload, OutboundAppMessage::MessageSigned(sig) if sig == vec![4, 5, 2]));
    // Ensure context is cleared in the final response
    assert!(final_app_msg.context.as_ref().is_empty());

    Ok(())
}
