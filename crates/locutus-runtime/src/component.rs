use crate::{util, ContractError, Runtime, RuntimeResult};
use locutus_stdlib::prelude::{
    ApplicationMessage, Component, ComponentContext, ComponentError, ComponentInterfaceResult,
    ComponentKey, GetSecretRequest, GetSecretResponse, InboundComponentMsg, OutboundComponentMsg,
    SetSecretRequest,
};

use crate::error::RuntimeInnerError;
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use std::collections::VecDeque;
use wasmer::{Instance, TypedFunction};

#[derive(thiserror::Error, Debug)]
pub enum ComponentExecError {
    #[error(transparent)]
    ComponentError(#[from] ComponentError),

    #[error("Received an unexpected message from the client apps: {0}")]
    UnexpectedMessage(&'static str),
}

pub trait ComponentRuntimeInterface {
    fn inbound_app_message(
        &mut self,
        key: &ComponentKey,
        inbound: Vec<InboundComponentMsg>,
    ) -> RuntimeResult<Vec<OutboundComponentMsg>>;

    fn register_component(
        &mut self,
        component: Component<'_>,
        cipher: XChaCha20Poly1305,
        nonce: XNonce,
    ) -> RuntimeResult<()>;

    fn unregister_component(&mut self, key: &ComponentKey) -> RuntimeResult<()>;
}

impl Runtime {
    fn exec_inbound(
        &mut self,
        msg: &InboundComponentMsg,
        process_func: &TypedFunction<i64, i64>,
        instance: &Instance,
    ) -> RuntimeResult<Vec<OutboundComponentMsg>> {
        let msg_ptr = {
            let msg = bincode::serialize(msg)?;
            let mut msg_buf = self.init_buf(instance, &msg)?;
            msg_buf.write(msg)?;
            msg_buf.ptr()
        };
        let res = process_func.call(&mut self.wasm_store, msg_ptr as i64)?;
        let linear_mem = self.linear_mem(instance)?;
        let outbound = unsafe {
            ComponentInterfaceResult::from_raw(res, &linear_mem)
                .unwrap(linear_mem)
                .map_err(Into::<ComponentExecError>::into)?
        };
        Ok(outbound)
    }

    // FIXME: modify the context atomically from the components, requires some changes to handle function calls with envs
    fn get_outbound(
        &mut self,
        component_key: &ComponentKey,
        instance: &Instance,
        process_func: &TypedFunction<i64, i64>,
        outbound_msgs: &mut VecDeque<OutboundComponentMsg>,
        results: &mut Vec<OutboundComponentMsg>,
    ) -> RuntimeResult<ComponentContext> {
        const MAX_ITERATIONS: usize = 100;

        let mut retries = 0;
        let Some(mut last_context) = outbound_msgs.back().and_then(|m| m.get_context().cloned()) else {
            return Ok(ComponentContext::default());
        };
        while let Some(outbound) = outbound_msgs.pop_front() {
            match outbound {
                OutboundComponentMsg::GetSecretRequest(GetSecretRequest {
                    key,
                    context,
                    processed,
                }) if !processed => {
                    let secret = self.secret_store.get_secret(component_key, &key)?;
                    let inbound = InboundComponentMsg::GetSecretResponse(GetSecretResponse {
                        key,
                        value: Some(secret),
                        context,
                    });
                    if retries >= MAX_ITERATIONS {
                        return Err(ContractError::from(RuntimeInnerError::ComponentExecError(ComponentError::Other("The maximum number of attempts to get the secret has been exceeded".to_string()).into())));
                    }
                    // OutboundComponentMsg::GetSecretRequest(GetSecretRequest { context, .. })
                    let new_msgs = self.exec_inbound(&inbound, process_func, instance)?;
                    retries += 1;
                    let Some(last_msg) = new_msgs.last() else {
                        return Err(ContractError::from(RuntimeInnerError::ComponentExecError(ComponentError::Other("Error trying to update the context from the secret".to_string()).into())));
                    };
                    let Some(new_last_context) = last_msg.get_context() else {
                        return Err(ContractError::from(RuntimeInnerError::ComponentExecError(ComponentError::Other("Last messsage ".to_string()).into())));
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
                OutboundComponentMsg::GetSecretRequest(GetSecretRequest { context, .. }) => {
                    last_context = context;
                }
                OutboundComponentMsg::SetSecretRequest(SetSecretRequest { key, value }) => {
                    if let Some(plaintext) = value {
                        self.secret_store
                            .store_secret(component_key, &key, plaintext)?;
                    } else {
                        self.secret_store.remove_secret(component_key, &key)?;
                    }
                }
                OutboundComponentMsg::ApplicationMessage(msg) if !msg.processed => {
                    if retries >= MAX_ITERATIONS {
                        panic!();
                    }
                    let outbound = self.exec_inbound(
                        &InboundComponentMsg::ApplicationMessage(
                            ApplicationMessage::new(msg.app, msg.payload, msg.processed)
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
                OutboundComponentMsg::ApplicationMessage(mut msg) => {
                    msg.context = ComponentContext::default();
                    results.push(OutboundComponentMsg::ApplicationMessage(msg));
                    break;
                }
                OutboundComponentMsg::RequestUserInput(req) => {
                    results.push(OutboundComponentMsg::RequestUserInput(req));
                    break;
                }
                OutboundComponentMsg::RandomBytesRequest(bytes) => {
                    let mut bytes = vec![0; bytes];
                    util::generate_random_bytes(&mut bytes);
                    let inbound = InboundComponentMsg::RandomBytes(bytes);
                    let new_outbound_msgs = self.exec_inbound(&inbound, process_func, instance)?;
                    for msg in new_outbound_msgs.into_iter() {
                        outbound_msgs.push_back(msg);
                    }
                }
            }
        }
        Ok(last_context)
    }
}

impl ComponentRuntimeInterface for Runtime {
    fn inbound_app_message(
        &mut self,
        key: &ComponentKey,
        inbound: Vec<InboundComponentMsg>,
    ) -> RuntimeResult<Vec<OutboundComponentMsg>> {
        let mut results = Vec::with_capacity(inbound.len());
        if inbound.is_empty() {
            return Ok(results);
        }
        let instance = self.prepare_component_call(key, 4096)?;
        let process_func: TypedFunction<i64, i64> = instance
            .exports
            .get_typed_function(&self.wasm_store, "process")?;

        // Initialize the shared context with the first message context
        let mut last_context = match inbound.first() {
            Some(msg) => {
                if let Some(context) = msg.get_context() {
                    context.clone()
                } else {
                    ComponentContext::default()
                }
            }
            _ => ComponentContext::default(),
        };

        for msg in inbound {
            match msg {
                InboundComponentMsg::ApplicationMessage(ApplicationMessage {
                    app,
                    payload,
                    processed,
                    ..
                }) => {
                    let mut outbound = VecDeque::from(
                        self.exec_inbound(
                            &InboundComponentMsg::ApplicationMessage(
                                ApplicationMessage::new(app, payload, processed)
                                    .with_context(last_context.clone()),
                            ),
                            &process_func,
                            &instance,
                        )?,
                    );

                    // Update the shared context for next messages
                    last_context = self.get_outbound(
                        key,
                        &instance,
                        &process_func,
                        &mut outbound,
                        &mut results,
                    )?;
                }
                InboundComponentMsg::UserResponse(response) => {
                    let mut outbound = VecDeque::from(self.exec_inbound(
                        &InboundComponentMsg::UserResponse(response),
                        &process_func,
                        &instance,
                    )?);
                    self.get_outbound(key, &instance, &process_func, &mut outbound, &mut results)?;
                }
                InboundComponentMsg::GetSecretResponse(_) => {
                    return Err(ComponentExecError::UnexpectedMessage("get secret response").into())
                }
                InboundComponentMsg::RandomBytes(bytes) => {
                    let mut outbound = VecDeque::from(self.exec_inbound(
                        &InboundComponentMsg::RandomBytes(bytes),
                        &process_func,
                        &instance,
                    )?);
                    self.get_outbound(key, &instance, &process_func, &mut outbound, &mut results)?;
                }
            }
        }
        Ok(results)
    }

    #[inline]
    fn register_component(
        &mut self,
        component: Component<'_>,
        cipher: XChaCha20Poly1305,
        nonce: XNonce,
    ) -> RuntimeResult<()> {
        self.secret_store
            .register_component(component.key().clone(), cipher, nonce)?;
        self.component_store.store_component(component)
    }

    #[inline]
    fn unregister_component(&mut self, key: &ComponentKey) -> RuntimeResult<()> {
        self.component_store.remove_component(key)
    }
}

#[cfg(test)]
mod test {
    use chacha20poly1305::aead::{AeadCore, KeyInit, OsRng};
    use locutus_stdlib::prelude::{env_logger, ContractCode, ContractInstanceId, Parameters};
    use serde::{Deserialize, Serialize};
    use std::{
        path::PathBuf,
        sync::{atomic::AtomicUsize, Arc},
    };

    use super::*;
    use crate::{component_store::ComponentStore, ContractStore, SecretsStore, WrappedContract};

    const TEST_COMPONENT_1: &str = "test_component_1";
    static TEST_NO: AtomicUsize = AtomicUsize::new(0);

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

    fn test_dir() -> PathBuf {
        let test_dir = std::env::temp_dir().join("locutus-test").join(format!(
            "component-api-test-{}",
            TEST_NO.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        ));
        if !test_dir.exists() {
            std::fs::create_dir_all(&test_dir).unwrap();
        }
        test_dir
    }

    fn get_test_component(name: &str) -> Result<Component, Box<dyn std::error::Error>> {
        let bytes = crate::tests::get_test_contract(name)?;
        Ok(Component::from(bytes))
    }

    fn set_up_runtime(name: &str) -> Result<(Component, Runtime), Box<dyn std::error::Error>> {
        let _ = env_logger::try_init();
        let contracts_dir = test_dir();
        let components_dir = test_dir();
        let secrets_dir = test_dir();

        let contract_store = ContractStore::new(contracts_dir, 10_000)?;
        let component_store = ComponentStore::new(components_dir, 10_000)?;
        let secret_store = SecretsStore::new(secrets_dir)?;

        let mut runtime =
            Runtime::build(contract_store, component_store, secret_store, false).unwrap();

        let component = get_test_component(name).unwrap();
        let _ = runtime.component_store.store_component(component.clone());

        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        let cipher = XChaCha20Poly1305::new(&key);
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let _ = runtime
            .secret_store
            .register_component(component.key().clone(), cipher, nonce);

        runtime.enable_wasi = true; // ENABLE FOR DEBUGGING; requires building for wasi
        Ok((component, runtime))
    }

    #[test]
    fn validate_process() -> Result<(), Box<dyn std::error::Error>> {
        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1])),
            Parameters::from(vec![]),
        );
        let (component, mut runtime) = set_up_runtime(TEST_COMPONENT_1)?;
        let app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

        // CreateInboxRequest message parts
        let payload: Vec<u8> = bincode::serialize(&InboundAppMessage::CreateInboxRequest).unwrap();
        let create_inbox_request_msg = ApplicationMessage::new(app, payload, false);

        let inbound = InboundComponentMsg::ApplicationMessage(create_inbox_request_msg);
        let outbound = runtime.inbound_app_message(component.key(), vec![inbound])?;
        let expected_payload =
            bincode::serialize(&OutboundAppMessage::CreateInboxResponse(vec![1])).unwrap();

        assert_eq!(outbound.len(), 1);
        assert!(matches!(
            outbound.get(0),
            Some(OutboundComponentMsg::ApplicationMessage(msg)) if *msg.payload == expected_payload
        ));

        // CreateInboxRequest message parts
        let payload: Vec<u8> =
            bincode::serialize(&InboundAppMessage::PleaseSignMessage(vec![1, 2, 3])).unwrap();
        let please_sign_message_msg = ApplicationMessage::new(app, payload, false);

        let inbound = InboundComponentMsg::ApplicationMessage(please_sign_message_msg);
        let outbound = runtime.inbound_app_message(component.key(), vec![inbound])?;
        let expected_payload =
            bincode::serialize(&OutboundAppMessage::MessageSigned(vec![4, 5, 2])).unwrap();
        assert_eq!(outbound.len(), 1);
        assert!(matches!(
            outbound.get(0),
            Some(OutboundComponentMsg::ApplicationMessage(msg)) if *msg.payload == expected_payload
        ));
        Ok(())
    }
}
