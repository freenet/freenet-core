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
        let res = process_func.call(
            &mut self.wasm_store,
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
        Ok(outbound)
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
        const MAX_ITERATIONS: usize = 100;
        let mut recurssion = 0;
        let Some(mut last_context) = outbound_msgs.back().and_then(|m| m.get_context().cloned())
        else {
            return Ok(DelegateContext::default());
        };
        while let Some(outbound) = outbound_msgs.pop_front() {
            match outbound {
                OutboundDelegateMsg::GetSecretRequest(GetSecretRequest {
                    key, processed, ..
                }) if !processed => {
                    let secret = self.secret_store.get_secret(delegate_key, &key)?;
                    let inbound = InboundDelegateMsg::GetSecretResponse(GetSecretResponse {
                        key,
                        value: Some(secret),
                        context: last_context.clone(),
                    });
                    if recurssion >= MAX_ITERATIONS {
                        return Err(ContractError::from(RuntimeInnerError::DelegateExecError(DelegateError::Other("The maximum number of attempts to get the secret has been exceeded".to_string()).into())));
                    }
                    let new_msgs =
                        self.exec_inbound(params, attested, &inbound, process_func, instance)?;
                    recurssion += 1;
                    let Some(last_msg) = new_msgs.last() else {
                        return Err(ContractError::from(RuntimeInnerError::DelegateExecError(
                            DelegateError::Other(
                                "Error trying to update the context from the secret".to_string(),
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
                        if !pending.processed() {
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
                OutboundDelegateMsg::ApplicationMessage(msg) if !msg.processed => {
                    if recurssion >= MAX_ITERATIONS {
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
                    recurssion += 1;
                    for msg in outbound {
                        outbound_msgs.push_back(msg);
                    }
                }
                OutboundDelegateMsg::ApplicationMessage(mut msg) => {
                    msg.context = DelegateContext::default();
                    results.push(OutboundDelegateMsg::ApplicationMessage(msg));
                    break;
                }
                OutboundDelegateMsg::RequestUserInput(req) => {
                    //results.push(OutboundDelegateMsg::RequestUserInput(req));
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
            .get_typed_function(&self.wasm_store, "process")?;

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
                    // FIXME: here only allow this if the application is trusted
                    // if attested.is_some() {
                    let secret = self.secret_store.get_secret(delegate_key, &secret_key)?;
                    let msg = OutboundDelegateMsg::GetSecretResponse(GetSecretResponse {
                        key: secret_key,
                        value: Some(secret),
                        context: Default::default(),
                    });
                    results.push(msg);
                    // } else {
                    //     return Err(DelegateExecError::UnauthorizedSecretAccess {
                    //         secret: secret_key.clone(),
                    //         delegate: delegate_key.clone(),
                    //     }
                    //     .into());
                    // }
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
                _ => unreachable!(),
            }
        }
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
        let secret_store = SecretsStore::new(secrets_dir)?;

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
