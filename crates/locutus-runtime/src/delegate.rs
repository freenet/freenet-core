use crate::{util, ContractError, Runtime, RuntimeResult};
use locutus_stdlib::prelude::{
    ApplicationMessage, ClientResponse, Delegate, DelegateContext, DelegateError,
    DelegateInterfaceResult, DelegateKey, GetSecretRequest, GetSecretResponse, InboundDelegateMsg,
    OutboundDelegateMsg, Parameters, SetSecretRequest,
};

use crate::error::RuntimeInnerError;
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use wasmer::{Instance, TypedFunction};

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
pub enum DelegateExecError {
    #[error(transparent)]
    DelegateError(#[from] DelegateError),

    #[error("Received an unexpected message from the client apps: {0}")]
    UnexpectedMessage(&'static str),
}

pub trait DelegateRuntimeInterface {
    fn inbound_app_message(
        &mut self,
        key: &DelegateKey,
        params: &Parameters,
        inbound: Vec<InboundDelegateMsg>,
    ) -> RuntimeResult<Vec<OutboundDelegateMsg>>;

    fn register_delegate(
        &mut self,
        delegate: Delegate<'_>,
        cipher: XChaCha20Poly1305,
        nonce: XNonce,
    ) -> RuntimeResult<()>;

    fn unregister_delegate(&mut self, key: &DelegateKey) -> RuntimeResult<()>;
}

impl Runtime {
    fn exec_inbound(
        &mut self,
        params: &Parameters<'_>,
        msg: &InboundDelegateMsg,
        process_func: &TypedFunction<(i64, i64), i64>,
        instance: &Instance,
    ) -> RuntimeResult<Vec<OutboundDelegateMsg>> {
        let param_buf_ptr = {
            let mut param_buf = self.init_buf(instance, params)?;
            param_buf.write(params)?;
            param_buf.ptr()
        };

        let msg_ptr = {
            let msg = bincode::serialize(msg)?;
            let mut msg_buf = self.init_buf(instance, &msg)?;
            msg_buf.write(msg)?;
            msg_buf.ptr()
        };
        let res = process_func.call(&mut self.wasm_store, param_buf_ptr as i64, msg_ptr as i64)?;
        let linear_mem = self.linear_mem(instance)?;
        let outbound = unsafe {
            DelegateInterfaceResult::from_raw(res, &linear_mem)
                .unwrap(linear_mem)
                .map_err(Into::<DelegateExecError>::into)?
        };
        Ok(outbound)
    }

    // FIXME: modify the context atomically from the delegates, requires some changes to handle function calls with envs
    fn get_outbound(
        &mut self,
        delegate_key: &DelegateKey,
        instance: &Instance,
        process_func: &TypedFunction<(i64, i64), i64>,
        params: &Parameters<'_>,
        outbound_msgs: &mut VecDeque<OutboundDelegateMsg>,
        results: &mut Vec<OutboundDelegateMsg>,
    ) -> RuntimeResult<DelegateContext> {
        const MAX_ITERATIONS: usize = 100;

        let mut retries = 0;
        let Some(mut last_context) = outbound_msgs.back().and_then(|m| m.get_context().cloned()) else {
            return Ok(DelegateContext::default());
        };
        while let Some(outbound) = outbound_msgs.pop_front() {
            match outbound {
                OutboundDelegateMsg::GetSecretRequest(GetSecretRequest {
                    key,
                    context,
                    processed,
                }) if !processed => {
                    let secret = self.secret_store.get_secret(delegate_key, &key)?;
                    let inbound = InboundDelegateMsg::GetSecretResponse(GetSecretResponse {
                        key,
                        value: Some(secret),
                        context,
                    });
                    if retries >= MAX_ITERATIONS {
                        return Err(ContractError::from(RuntimeInnerError::DelegateExecError(DelegateError::Other("The maximum number of attempts to get the secret has been exceeded".to_string()).into())));
                    }
                    let new_msgs = self.exec_inbound(&params, &inbound, process_func, instance)?;
                    retries += 1;
                    let Some(last_msg) = new_msgs.last() else {
                        return Err(ContractError::from(RuntimeInnerError::DelegateExecError(DelegateError::Other("Error trying to update the context from the secret".to_string()).into())));
                    };
                    let Some(new_last_context) = last_msg.get_context() else {
                        return Err(ContractError::from(RuntimeInnerError::DelegateExecError(DelegateError::Other("Last messsage ".to_string()).into())));
                    };
                    last_context = new_last_context.clone();
                    for mut pending in new_msgs {
                        if let Some(ctx) = pending.get_mut_context() {
                            *ctx = last_context.clone();
                        };
                        if !pending.processed() {
                            outbound_msgs.push_back(pending);
                        }
                    }
                }
                OutboundDelegateMsg::GetSecretRequest(GetSecretRequest { context, .. }) => {
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
                    if retries >= MAX_ITERATIONS {
                        panic!();
                    }
                    let outbound = self.exec_inbound(
                        &params,
                        &InboundDelegateMsg::ApplicationMessage(
                            ApplicationMessage::new(msg.app, msg.payload)
                                .processed(msg.processed)
                                .with_context(last_context.clone()),
                        ),
                        process_func,
                        instance,
                    )?;
                    retries += 1;
                    for m in outbound {
                        outbound_msgs.push_back(m);
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
                    let mut context: Context =
                        bincode::deserialize(last_context.0.as_slice()).unwrap();
                    context.waiting_for_user_input.remove(&req_id);
                    context.user_response.insert(req_id, response);
                    last_context = DelegateContext::new(bincode::serialize(&context).unwrap());
                }
                OutboundDelegateMsg::RandomBytesRequest(bytes) => {
                    let mut bytes = vec![0; bytes];
                    util::generate_random_bytes(&mut bytes);
                    let inbound = InboundDelegateMsg::RandomBytes(bytes);
                    let new_outbound_msgs =
                        self.exec_inbound(&params, &inbound, process_func, instance)?;
                    for msg in new_outbound_msgs.into_iter() {
                        outbound_msgs.push_back(msg);
                    }
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
        key: &DelegateKey,
        params: &Parameters,
        inbound: Vec<InboundDelegateMsg>,
    ) -> RuntimeResult<Vec<OutboundDelegateMsg>> {
        let mut results = Vec::with_capacity(inbound.len());
        if inbound.is_empty() {
            return Ok(results);
        }
        let running = self.prepare_delegate_call(params, key, 4096)?;
        let process_func: TypedFunction<(i64, i64), i64> = running
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

        for msg in inbound {
            match msg {
                InboundDelegateMsg::ApplicationMessage(ApplicationMessage {
                    app,
                    payload,
                    processed,
                    ..
                }) => {
                    let mut outbound = VecDeque::from(
                        self.exec_inbound(
                            &params,
                            &InboundDelegateMsg::ApplicationMessage(
                                ApplicationMessage::new(app, payload)
                                    .processed(processed)
                                    .with_context(last_context.clone()),
                            ),
                            &process_func,
                            &running.instance,
                        )?,
                    );

                    // Update the shared context for next messages
                    last_context = self.get_outbound(
                        key,
                        &running.instance,
                        &process_func,
                        &params,
                        &mut outbound,
                        &mut results,
                    )?;
                }
                InboundDelegateMsg::UserResponse(response) => {
                    let mut outbound = VecDeque::from(self.exec_inbound(
                        &params,
                        &InboundDelegateMsg::UserResponse(response),
                        &process_func,
                        &running.instance,
                    )?);
                    self.get_outbound(
                        key,
                        &running.instance,
                        &process_func,
                        &params,
                        &mut outbound,
                        &mut results,
                    )?;
                }
                InboundDelegateMsg::GetSecretResponse(_) => {
                    return Err(DelegateExecError::UnexpectedMessage("get secret response").into())
                }
                InboundDelegateMsg::RandomBytes(bytes) => {
                    let mut outbound = VecDeque::from(self.exec_inbound(
                        &params,
                        &InboundDelegateMsg::RandomBytes(bytes),
                        &process_func,
                        &running.instance,
                    )?);
                    self.get_outbound(
                        key,
                        &running.instance,
                        &process_func,
                        &params,
                        &mut outbound,
                        &mut results,
                    )?;
                }
            }
        }
        Ok(results)
    }

    #[inline]
    fn register_delegate(
        &mut self,
        delegate: Delegate<'_>,
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
    use locutus_stdlib::prelude::{ContractCode, ContractInstanceId, Parameters};
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    use super::*;
    use crate::{delegate_store::DelegateStore, ContractStore, SecretsStore, WrappedContract};

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

    fn setup_runtime(name: &str) -> Result<(Delegate, Runtime), Box<dyn std::error::Error>> {
        const TEST_PREFIX: &str = "delegate-api";
        let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();
        let contracts_dir = crate::tests::test_dir(TEST_PREFIX);
        let delegates_dir = crate::tests::test_dir(TEST_PREFIX);
        let secrets_dir = crate::tests::test_dir(TEST_PREFIX);

        let contract_store = ContractStore::new(contracts_dir, 10_000)?;
        let delegate_store = DelegateStore::new(delegates_dir, 10_000)?;
        let secret_store = SecretsStore::new(secrets_dir)?;

        let mut runtime =
            Runtime::build(contract_store, delegate_store, secret_store, false).unwrap();

        let delegate = {
            let bytes = crate::tests::get_test_module(name)?;
            Delegate::from((&bytes.into(), &vec![].into()))
        };
        let _ = runtime.delegate_store.store_delegate(delegate.clone());

        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        let cipher = XChaCha20Poly1305::new(&key);
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let _ = runtime
            .secret_store
            .register_delegate(delegate.key().clone(), cipher, nonce);

        runtime.enable_wasi = true; // ENABLE FOR DEBUGGING; requires building for wasi
        Ok((delegate, runtime))
    }

    #[test]
    fn validate_process() -> Result<(), Box<dyn std::error::Error>> {
        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1])),
            Parameters::from(vec![]),
        );
        let (delegate, mut runtime) = setup_runtime(TEST_DELEGATE_1)?;
        let app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

        // CreateInboxRequest message parts
        let payload: Vec<u8> = bincode::serialize(&InboundAppMessage::CreateInboxRequest).unwrap();
        let create_inbox_request_msg = ApplicationMessage::new(app, payload);

        let inbound = InboundDelegateMsg::ApplicationMessage(create_inbox_request_msg);
        let outbound =
            runtime.inbound_app_message(delegate.key(), &vec![].into(), vec![inbound])?;
        let expected_payload =
            bincode::serialize(&OutboundAppMessage::CreateInboxResponse(vec![1])).unwrap();

        assert_eq!(outbound.len(), 1);
        assert!(matches!(
            outbound.get(0),
            Some(OutboundDelegateMsg::ApplicationMessage(msg)) if *msg.payload == expected_payload
        ));

        // CreateInboxRequest message parts
        let payload: Vec<u8> =
            bincode::serialize(&InboundAppMessage::PleaseSignMessage(vec![1, 2, 3])).unwrap();
        let please_sign_message_msg = ApplicationMessage::new(app, payload);

        let inbound = InboundDelegateMsg::ApplicationMessage(please_sign_message_msg);
        let outbound =
            runtime.inbound_app_message(delegate.key(), &vec![].into(), vec![inbound])?;
        let expected_payload =
            bincode::serialize(&OutboundAppMessage::MessageSigned(vec![4, 5, 2])).unwrap();
        assert_eq!(outbound.len(), 1);
        assert!(matches!(
            outbound.get(0),
            Some(OutboundDelegateMsg::ApplicationMessage(msg)) if *msg.payload == expected_payload
        ));
        Ok(())
    }
}
