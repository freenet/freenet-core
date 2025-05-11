use std::collections::{HashMap, HashSet, VecDeque};

use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use freenet_stdlib::prelude::{
    ApplicationMessage, ClientResponse, DelegateContainer, DelegateContext, DelegateError,
    DelegateInterfaceResult, DelegateKey, GetSecretRequest, GetSecretResponse, InboundDelegateMsg,
    OutboundDelegateMsg, Parameters, SecretsId, SetSecretRequest,
};
use serde::{Deserialize, Serialize};
use wasmer::{Instance, TypedFunction};

use super::error::RuntimeInnerError;
use super::{ContractError, Runtime, RuntimeResult};

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Allowed,
    NotAllowed,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct Context {
    waiting_for_user_input: HashSet<u32>,
    user_response: HashMap<u32, Response>,
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum DelegateExecError {
    #[error(transparent)]
    DelegateError(#[from] DelegateError),

    #[error("Permission denied: secret {secret} cannot be accesed by {delegate} at this time")]
    UnauthorizedSecretAccess {
        secret: SecretsId,
        delegate: DelegateKey,
    },

    #[error("Received an unexpected message from the client apps: {0}")]
    UnexpectedMessage(&'static str),
}

pub(crate) trait DelegateRuntimeInterface {
    fn inbound_app_message(
        &mut self,
        key: &DelegateKey,
        params: &Parameters,
        attested: Option<&[u8]>,
        inbound: Vec<InboundDelegateMsg>,
    ) -> RuntimeResult<Vec<OutboundDelegateMsg>>;

    fn register_delegate(
        &mut self,
        delegate: DelegateContainer,
        cipher: XChaCha20Poly1305,
        nonce: XNonce,
    ) -> RuntimeResult<()>;

    fn unregister_delegate(&mut self, key: &DelegateKey) -> RuntimeResult<()>;
}

impl Runtime {
    fn exec_inbound(
        &mut self,
        params: &Parameters<'_>,
        attested: Option<&[u8]>,
        msg: &InboundDelegateMsg,
        process_func: &TypedFunction<(i64, i64, i64), i64>,
        instance: &Instance,
    ) -> RuntimeResult<Vec<OutboundDelegateMsg>> {
        let param_buf_ptr = {
            let mut param_buf = self.init_buf(instance, params)?;
            param_buf.write(params)?;
            param_buf.ptr()
        };
        let attested_buf_ptr = {
            let bytes = attested.unwrap_or(&[]);
            let mut attested_buf = self.init_buf(instance, bytes)?;
            attested_buf.write(bytes)?;
            attested_buf.ptr()
        };
        let msg_ptr = {
            let msg = bincode::serialize(msg)?;
            let mut msg_buf = self.init_buf(instance, &msg)?;
            msg_buf.write(msg)?;
            msg_buf.ptr()
        };
        let inbound_msg_name = match msg {
            InboundDelegateMsg::ApplicationMessage(_) => "ApplicationMessage",
            InboundDelegateMsg::UserResponse(_) => "UserResponse",
            InboundDelegateMsg::GetSecretResponse(_) => "GetSecretResponse",
            InboundDelegateMsg::GetSecretRequest(_) => "GetSecretRequest",
        };
        tracing::debug!(inbound_msg_name, "Calling delegate with inbound message");
        let res = process_func.call(
            self.wasm_store.as_mut().unwrap(),
            param_buf_ptr as i64,
            attested_buf_ptr as i64,
            msg_ptr as i64,
        )?;
        let linear_mem = self.linear_mem(instance)?;
        let outbound = unsafe {
            DelegateInterfaceResult::from_raw(res, &linear_mem)
                .unwrap(linear_mem)
                .map_err(Into::<DelegateExecError>::into)?
        };
        self.log_delegate_exec_result(inbound_msg_name, &outbound);
        Ok(outbound)
    }

    fn log_delegate_exec_result(&self, inbound_msg_name: &str, outbound: &[OutboundDelegateMsg]) {
        if tracing::enabled!(tracing::Level::DEBUG) {
            let outbound_message_names = outbound
                .iter()
                .map(|m| match m {
                    OutboundDelegateMsg::ApplicationMessage(am) => format!(
                        "ApplicationMessage(app={}, payload_len={}, processed={}, context_len={})",
                        am.app,
                        am.payload.len(),
                        am.processed,
                        am.context.as_ref().len()
                    ),
                    OutboundDelegateMsg::RequestUserInput(_) => "RequestUserInput".to_string(),
                    OutboundDelegateMsg::ContextUpdated(_) => "ContextUpdated".to_string(),
                    OutboundDelegateMsg::GetSecretRequest(_) => "GetSecretRequest".to_string(),
                    OutboundDelegateMsg::SetSecretRequest(_) => "SetSecretRequest".to_string(),
                    OutboundDelegateMsg::GetSecretResponse(_) => "GetSecretResponse".to_string(),
                })
                .collect::<Vec<String>>()
                .join(", ");
            tracing::debug!(
                inbound_msg_name,
                outbound_message_names,
                "Delegate returned outbound messages"
            );
        } else {
            tracing::debug!(
                inbound_msg_name,
                outbound_len = outbound.len(),
                "Delegate returned outbound messages"
            );
        }
    }

    fn log_get_outbound_entry(
        &self,
        delegate_key: &DelegateKey,
        attested: Option<&[u8]>,
        outbound_msgs: &VecDeque<OutboundDelegateMsg>,
    ) {
        tracing::debug!(
            delegate_key = ?delegate_key,
            ?attested,
            outbound_msgs_len = outbound_msgs.len(),
            // Generate message details only if DEBUG level is enabled
            outbound_msg_details = debug(if tracing::enabled!(tracing::Level::DEBUG) {
                outbound_msgs.iter().map(|msg| {
                    match msg {
                        OutboundDelegateMsg::ApplicationMessage(m) => format!("AppMsg(payload_len={})", m.payload.len()),
                        OutboundDelegateMsg::GetSecretRequest(_) => "GetSecretReq".to_string(),
                        OutboundDelegateMsg::GetSecretResponse(_) => "GetSecretResp".to_string(),
                        OutboundDelegateMsg::SetSecretRequest(_) => "SetSecretReq".to_string(),
                        OutboundDelegateMsg::RequestUserInput(_) => "UserInputReq".to_string(),
                        OutboundDelegateMsg::ContextUpdated(_) => "ContextUpdate".to_string(),
                    }
                }).collect::<Vec<_>>()
            } else {
                // Avoid computation if tracing level is disabled
                Vec::new()
            }),
            "get_outbound called"
        );
    }

    // FIXME: modify the context atomically from the delegates, requires some changes to handle function calls with envs
    #[allow(clippy::too_many_arguments)]
    fn get_outbound(
        &mut self,
        delegate_key: &DelegateKey,
        instance: &Instance,
        process_func: &TypedFunction<(i64, i64, i64), i64>,
        params: &Parameters<'_>,
        attested: Option<&[u8]>,
        outbound_msgs: &mut VecDeque<OutboundDelegateMsg>,
        results: &mut Vec<OutboundDelegateMsg>,
    ) -> RuntimeResult<DelegateContext> {
        self.log_get_outbound_entry(delegate_key, attested, outbound_msgs);

        const MAX_ITERATIONS: usize = 100;
        let mut recursion = 0;
        let Some(mut last_context) = outbound_msgs.back().and_then(|m| m.get_context().cloned())
        else {
            return Ok(DelegateContext::default());
        };
        while let Some(outbound) = outbound_msgs.pop_front() {
            match outbound {
                OutboundDelegateMsg::GetSecretRequest(GetSecretRequest {
                    key, processed, ..
                }) if !processed => {
                    tracing::debug!(%key, "Handling OutboundDelegateMsg::GetSecretRequest received from delegate");
                    let secret_result = self.secret_store.get_secret(delegate_key, &key).ok();
                    tracing::debug!(%key, secret_is_some = secret_result.is_some(), "Secret store responded");
                    let inbound = InboundDelegateMsg::GetSecretResponse(GetSecretResponse {
                        key,
                        value: secret_result,
                        context: last_context.clone(),
                    });
                    if recursion >= MAX_ITERATIONS {
                        return Err(ContractError::from(RuntimeInnerError::DelegateExecError(DelegateError::Other("The maximum number of attempts to get the secret has been exceeded".to_string()).into())));
                    }
                    let new_msgs =
                        self.exec_inbound(params, attested, &inbound, process_func, instance)?;
                    if tracing::enabled!(tracing::Level::DEBUG) {
                        let summary = new_msgs
                            .iter()
                            .map(|m| match m {
                                OutboundDelegateMsg::ApplicationMessage(_) => "ApplicationMessage",
                                OutboundDelegateMsg::RequestUserInput(_) => "RequestUserInput",
                                OutboundDelegateMsg::ContextUpdated(_) => "ContextUpdated",
                                OutboundDelegateMsg::GetSecretRequest(_) => "GetSecretRequest",
                                OutboundDelegateMsg::SetSecretRequest(_) => "SetSecretRequest",
                                OutboundDelegateMsg::GetSecretResponse(_) => "GetSecretResponse",
                            })
                            .collect::<Vec<_>>();
                        tracing::debug!(
                            count = new_msgs.len(),
                            ?summary,
                            "Messages returned from exec_inbound after GetSecretResponse"
                        );
                    }
                    recursion += 1;
                    let Some(last_msg) = new_msgs.last() else {
                        return Err(ContractError::from(RuntimeInnerError::DelegateExecError(
                            DelegateError::Other(
                                "Delegate did not return any messages after GetSecretResponse"
                                    .to_string(),
                            )
                            .into(),
                        )));
                    };
                    let mut updated = false;
                    if let Some(new_last_context) = last_msg.get_context() {
                        last_context = new_last_context.clone();
                        updated = true;
                    };
                    for mut pending in new_msgs {
                        if updated {
                            if let Some(ctx) = pending.get_mut_context() {
                                *ctx = last_context.clone();
                            };
                        }
                        // Check if the message is processed AND an ApplicationMessage
                        if pending.processed() {
                            if let OutboundDelegateMsg::ApplicationMessage(mut app_msg) = pending {
                                // Add processed ApplicationMessages directly to results
                                tracing::debug!(app = %app_msg.app, payload_len = app_msg.payload.len(), "Adding processed ApplicationMessage from GetSecretResponse handling to results");
                                app_msg.context = DelegateContext::default(); // Ensure context is cleared
                                results.push(OutboundDelegateMsg::ApplicationMessage(app_msg));
                            } else {
                                // Handle other processed messages if necessary (currently none expected here)
                                tracing::warn!(?pending, "Ignoring unexpected processed message during GetSecretRequest handling");
                            }
                        } else {
                            // Push non-processed messages back for further handling
                            outbound_msgs.push_back(pending);
                        }
                    }
                }
                OutboundDelegateMsg::GetSecretRequest(GetSecretRequest { context, .. }) => {
                    tracing::debug!("get secret, processed");
                    last_context = context;
                }
                OutboundDelegateMsg::GetSecretResponse(GetSecretResponse { context, .. }) => {
                    last_context = context;
                }
                OutboundDelegateMsg::SetSecretRequest(SetSecretRequest { key, value }) => {
                    if let Some(plaintext) = value {
                        self.secret_store
                            .store_secret(delegate_key, &key, plaintext)?;
                    } else {
                        self.secret_store.remove_secret(delegate_key, &key)?;
                    }
                }
                /*  Why would it take the payload from an ApplicationMessage coming from the delegate and
                    send it back to the delegate?

                OutboundDelegateMsg::ApplicationMessage(msg) if !msg.processed => {
                         if recursion >= MAX_ITERATIONS {
                             return Err(DelegateExecError::DelegateError(DelegateError::Other(
                                 "max recurssion (100) limit hit".into(),
                             ))
                             .into());
                         }
                         let outbound = self.exec_inbound(
                             params,
                             attested,
                             &InboundDelegateMsg::ApplicationMessage(
                                 ApplicationMessage::new(msg.app, msg.payload)
                                     .processed(msg.processed)
                                     .with_context(last_context.clone()),
                             ),
                             process_func,
                             instance,
                         )?;
                         recursion += 1;
                         for msg in outbound {
                             outbound_msgs.push_back(msg);
                         }
                     } */
                OutboundDelegateMsg::ApplicationMessage(mut msg) => {
                    tracing::debug!(app = %msg.app, payload_len = msg.payload.len(), processed = msg.processed, "Adding processed ApplicationMessage to results in get_outbound");
                    msg.context = DelegateContext::default();
                    results.push(OutboundDelegateMsg::ApplicationMessage(msg));
                    break;
                }
                OutboundDelegateMsg::RequestUserInput(req) => {
                    // Simulate user response changes after receiving the RequestUserInput
                    let user_response =
                        ClientResponse::new(serde_json::to_vec(&Response::Allowed).unwrap());
                    let response: Response = serde_json::from_slice(&user_response)
                        .map_err(|err| DelegateError::Deser(format!("{err}")))
                        .unwrap();
                    let req_id = req.request_id;
                    let mut context: Context = bincode::deserialize(last_context.as_ref()).unwrap();
                    context.waiting_for_user_input.remove(&req_id);
                    context.user_response.insert(req_id, response);
                    last_context = DelegateContext::new(bincode::serialize(&context).unwrap());
                }
                OutboundDelegateMsg::ContextUpdated(context) => {
                    last_context = context;
                }
            }
        }
        Ok(last_context)
    }
}

impl DelegateRuntimeInterface for Runtime {
    fn inbound_app_message(
        &mut self,
        delegate_key: &DelegateKey,
        params: &Parameters,
        attested: Option<&[u8]>,
        inbound: Vec<InboundDelegateMsg>,
    ) -> RuntimeResult<Vec<OutboundDelegateMsg>> {
        let mut results = Vec::with_capacity(inbound.len());
        if inbound.is_empty() {
            return Ok(results);
        }
        let running = self.prepare_delegate_call(params, delegate_key, 4096)?;
        let process_func: TypedFunction<(i64, i64, i64), i64> = running
            .instance
            .exports
            .get_typed_function(self.wasm_store.as_ref().unwrap(), "process")?;

        // Initialize the shared context with the first message context
        let mut last_context = match inbound.first() {
            Some(msg) => {
                if let Some(context) = msg.get_context() {
                    context.clone()
                } else {
                    DelegateContext::default()
                }
            }
            _ => DelegateContext::default(),
        };

        let mut non_processed = vec![];
        for msg in inbound {
            match msg {
                InboundDelegateMsg::ApplicationMessage(ApplicationMessage {
                    app,
                    payload,
                    processed,
                    ..
                }) => {
                    let outbound = VecDeque::from(
                        self.exec_inbound(
                            params,
                            attested,
                            &InboundDelegateMsg::ApplicationMessage(
                                ApplicationMessage::new(app, payload)
                                    .processed(processed)
                                    .with_context(last_context.clone()),
                            ),
                            &process_func,
                            &running.instance,
                        )?,
                    );

                    let mut real_outbound = VecDeque::new();
                    for outbound in outbound {
                        match outbound {
                            OutboundDelegateMsg::SetSecretRequest(set) => {
                                non_processed.push(OutboundDelegateMsg::SetSecretRequest(set));
                            }
                            // msg if !msg.processed() {}
                            msg => real_outbound.push_back(msg),
                        }
                    }

                    // Update the shared context for next messages
                    last_context = self.get_outbound(
                        delegate_key,
                        &running.instance,
                        &process_func,
                        params,
                        attested,
                        &mut real_outbound,
                        &mut results,
                    )?;
                }
                InboundDelegateMsg::UserResponse(response) => {
                    let outbound = self.exec_inbound(
                        params,
                        attested,
                        &InboundDelegateMsg::UserResponse(response),
                        &process_func,
                        &running.instance,
                    )?;

                    let mut real_outbound = VecDeque::new();
                    for outbound in outbound {
                        match outbound {
                            msg if !msg.processed() => non_processed.push(msg),
                            msg => real_outbound.push_back(msg),
                        }
                    }

                    self.get_outbound(
                        delegate_key,
                        &running.instance,
                        &process_func,
                        params,
                        attested,
                        &mut real_outbound,
                        &mut results,
                    )?;
                }
                InboundDelegateMsg::GetSecretRequest(GetSecretRequest {
                    key: secret_key, ..
                }) => {
                    if attested.is_some() {
                        let secret = self.secret_store.get_secret(delegate_key, &secret_key)?;
                        let msg = OutboundDelegateMsg::GetSecretResponse(GetSecretResponse {
                            key: secret_key,
                            value: Some(secret),
                            context: Default::default(),
                        });
                        results.push(msg);
                    } else {
                        return Err(DelegateExecError::UnauthorizedSecretAccess {
                            secret: secret_key.clone(),
                            delegate: delegate_key.clone(),
                        }
                        .into());
                    }
                }
                _ => {}
            }
        }
        for outbound in non_processed {
            match outbound {
                OutboundDelegateMsg::SetSecretRequest(SetSecretRequest { key, value }) => {
                    if let Some(plaintext) = value {
                        self.secret_store
                            .store_secret(delegate_key, &key, plaintext)?;
                    } else {
                        self.secret_store.remove_secret(delegate_key, &key)?;
                    }
                }
                _ => unreachable!("All OutboundDelegateMsg variants should be handled"),
            }
        }
        tracing::debug!(
            count = results.len(),
            "Final results returned by inbound_app_message"
        );
        Ok(results)
    }

    #[inline]
    fn register_delegate(
        &mut self,
        delegate: DelegateContainer,
        cipher: XChaCha20Poly1305,
        nonce: XNonce,
    ) -> RuntimeResult<()> {
        self.secret_store
            .register_delegate(delegate.key().clone(), cipher, nonce)?;
        self.delegate_store.store_delegate(delegate)
    }

    #[inline]
    fn unregister_delegate(&mut self, key: &DelegateKey) -> RuntimeResult<()> {
        self.delegate_store.remove_delegate(key)
    }
}

#[cfg(test)]
mod test {
    use chacha20poly1305::aead::{AeadCore, KeyInit, OsRng};
    use freenet_stdlib::prelude::*;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    use crate::util::tests::get_temp_dir;

    use super::super::{delegate_store::DelegateStore, ContractStore, SecretsStore};
    use super::*;

    const TEST_DELEGATE_1: &str = "test_delegate_1";

    #[derive(Debug, Serialize, Deserialize)]
    struct SecretsContext {
        private_key: Option<Vec<u8>>,
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

    fn setup_runtime(
        name: &str,
    ) -> Result<(DelegateContainer, Runtime, tempfile::TempDir), Box<dyn std::error::Error>> {
        // let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();
        let temp_dir = get_temp_dir();
        let contracts_dir = temp_dir.path().join("contracts");
        let delegates_dir = temp_dir.path().join("delegates");
        let secrets_dir = temp_dir.path().join("secrets");

        let contract_store = ContractStore::new(contracts_dir, 10_000)?;
        let delegate_store = DelegateStore::new(delegates_dir, 10_000)?;
        let secret_store = SecretsStore::new(secrets_dir, Default::default())?;

        let mut runtime =
            Runtime::build(contract_store, delegate_store, secret_store, false).unwrap();

        let delegate = {
            let bytes = super::super::tests::get_test_module(name)?;
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate::from((
                &bytes.into(),
                &vec![].into(),
            ))))
        };
        let _ = runtime.delegate_store.store_delegate(delegate.clone());

        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        let cipher = XChaCha20Poly1305::new(&key);
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let _ = runtime
            .secret_store
            .register_delegate(delegate.key().clone(), cipher, nonce);

        Ok((delegate, runtime, temp_dir))
    }

    #[test]
    fn validate_process() -> Result<(), Box<dyn std::error::Error>> {
        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1])),
            Parameters::from(vec![]),
        );
        let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_1)?;
        let app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

        // CreateInboxRequest message parts
        let payload: Vec<u8> = bincode::serialize(&InboundAppMessage::CreateInboxRequest).unwrap();
        let create_inbox_request_msg = ApplicationMessage::new(app, payload);

        let inbound = InboundDelegateMsg::ApplicationMessage(create_inbox_request_msg);
        let outbound =
            runtime.inbound_app_message(delegate.key(), &vec![].into(), None, vec![inbound])?;
        let expected_payload =
            bincode::serialize(&OutboundAppMessage::CreateInboxResponse(vec![1])).unwrap();

        assert_eq!(outbound.len(), 1);
        assert!(matches!(
            outbound.first(),
            Some(OutboundDelegateMsg::ApplicationMessage(msg)) if *msg.payload == expected_payload
        ));

        // CreateInboxRequest message parts
        let payload: Vec<u8> =
            bincode::serialize(&InboundAppMessage::PleaseSignMessage(vec![1, 2, 3])).unwrap();
        let please_sign_message_msg = ApplicationMessage::new(app, payload);

        let inbound = InboundDelegateMsg::ApplicationMessage(please_sign_message_msg);
        let outbound =
            runtime.inbound_app_message(delegate.key(), &vec![].into(), None, vec![inbound])?;
        let expected_payload =
            bincode::serialize(&OutboundAppMessage::MessageSigned(vec![4, 5, 2])).unwrap();
        assert_eq!(outbound.len(), 1);
        assert!(matches!(
            outbound.first(),
            Some(OutboundDelegateMsg::ApplicationMessage(msg)) if *msg.payload == expected_payload
        ));
        std::mem::drop(temp_dir);
        Ok(())
    }
}
