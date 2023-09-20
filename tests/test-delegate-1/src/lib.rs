use freenet_stdlib::prelude::*;
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

impl From<SecretsContext> for DelegateContext {
    fn from(val: SecretsContext) -> Self {
        DelegateContext::new(val.serialized())
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
                        }

                        let secrets_context: SecretsContext =
                            bincode::deserialize(incoming_app.context.as_ref())
                                .map_err(|err| DelegateError::Other(format!("{err}")))?;

                        if let Some(_private_key) = secrets_context.private_key {
                            // Response with signature to the application
                            let signature = vec![4, 5, 2];
                            let response_msg_content: OutboundAppMessage =
                                OutboundAppMessage::MessageSigned(signature);
                            let payload: Vec<u8> = bincode::serialize(&response_msg_content)
                                .map_err(|err| DelegateError::Other(format!("{err}")))?;
                            let response_app_msg =
                                ApplicationMessage::new(incoming_app.app, payload)
                                    .processed(true)
                                    .with_context(incoming_app.context);
                            outbound
                                .push(OutboundDelegateMsg::ApplicationMessage(response_app_msg));
                        } else {
                            // Secret request
                            let get_secret_request_msg =
                                OutboundDelegateMsg::GetSecretRequest(GetSecretRequest {
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
                                ApplicationMessage::new(app, payload)
                                    .processed(false)
                                    .with_context(context);
                            outbound.push(OutboundDelegateMsg::ApplicationMessage(
                                please_sign_message_content,
                            ))
                        }
                    }
                }
            }
            InboundDelegateMsg::GetSecretResponse(secret_response) => {
                // Response with signature to the application
                let pk_bytes = secret_response.value.unwrap();
                let serialized_context: Vec<u8> = bincode::serialize(&SecretsContext {
                    private_key: Some(pk_bytes),
                })
                .map_err(|err| DelegateError::Other(format!("{err}")))?;

                // Secret request
                let get_secret_request_msg =
                    OutboundDelegateMsg::GetSecretRequest(GetSecretRequest {
                        key: secret_response.key,
                        context: DelegateContext::new(serialized_context),
                        processed: true,
                    });
                outbound.push(get_secret_request_msg);
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
    let output = Delegate::process(Parameters::from(vec![]), inbound)?;
    assert_eq!(output.len(), 2);
    assert!(matches!(
        output.first().unwrap(),
        OutboundDelegateMsg::SetSecretRequest(_)
    ));
    assert!(matches!(
        output.last().unwrap(),
        OutboundDelegateMsg::ApplicationMessage(_)
    ));

    // 2. request sign message
    let payload: Vec<u8> =
        bincode::serialize(&InboundAppMessage::PleaseSignMessage(PRIVATE_KEY.to_vec())).unwrap();
    let id = ContractInstanceId::try_from(['a'; 32].into_iter().collect::<String>()).unwrap();
    let sign_msg = ApplicationMessage::new(id, payload).processed(false);
    let output = Delegate::process(
        Parameters::from(vec![]),
        InboundDelegateMsg::ApplicationMessage(sign_msg),
    )?;
    assert_eq!(output.len(), 2);
    assert!(matches!(
        output.first().unwrap(),
        OutboundDelegateMsg::GetSecretRequest(_)
    ));
    assert!(matches!(
        output.last().unwrap(),
        OutboundDelegateMsg::ApplicationMessage(_)
    ));

    // 3. sign up after getting key
    let msg = InboundDelegateMsg::GetSecretResponse(GetSecretResponse {
        key: SecretsId::new(PRIVATE_KEY.to_vec()),
        value: Some(PRIVATE_KEY.to_vec()),
        context: DelegateContext::default(),
    });
    let output = Delegate::process(Parameters::from(vec![]), msg)?;
    assert_eq!(output.len(), 1);
    match output.first().unwrap() {
        OutboundDelegateMsg::GetSecretRequest(GetSecretRequest {
            context, processed, ..
        }) if *processed => {
            let ctx: SecretsContext = bincode::deserialize(context.as_ref())?;
            assert!(matches!(ctx.private_key, Some(v) if v == PRIVATE_KEY));
        }
        _ => return Err("Not expected output".into()),
    };

    Ok(())
}
