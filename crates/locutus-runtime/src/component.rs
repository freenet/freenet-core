use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use locutus_stdlib::prelude::{
    ApplicationMessage, Component, ComponentError, ComponentInterfaceResult, ComponentKey,
    GetSecretRequest, GetSecretResponse, InboundComponentMsg, OutboundComponentMsg,
    SetSecretRequest,
};
use wasmer::{Instance, TypedFunction};

use crate::{util, Runtime, RuntimeResult};

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

    // FIXME: control the use of recurssion here since is a potential exploit for malicious components
    fn get_outbound(
        &mut self,
        component_key: &ComponentKey,
        instance: &Instance,
        process_func: &TypedFunction<i64, i64>,
        outbound_msgs: Vec<OutboundComponentMsg>,
        results: &mut Vec<OutboundComponentMsg>,
    ) -> RuntimeResult<()> {
        for outbound in outbound_msgs {
            match outbound {
                OutboundComponentMsg::GetSecretRequest(GetSecretRequest {
                    key, context, ..
                }) => {
                    let secret = self.secret_store.get_secret(component_key, &key)?;
                    let inbound = InboundComponentMsg::GetSecretResponse(GetSecretResponse {
                        key,
                        value: Some(secret),
                        context,
                    });
                    let outbound_msgs = self.exec_inbound(&inbound, process_func, instance)?;
                    self.get_outbound(
                        component_key,
                        instance,
                        process_func,
                        outbound_msgs,
                        results,
                    )?;
                }
                OutboundComponentMsg::SetSecretRequest(SetSecretRequest { key, value }) => {
                    if let Some(plaintext) = value {
                        self.secret_store
                            .store_secret(component_key, &key, plaintext)?;
                    } else {
                        self.secret_store.remove_secret(component_key, &key)?;
                    }
                    break;
                }
                OutboundComponentMsg::ApplicationMessage(msg) => {
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
                    let outbound_msgs = self.exec_inbound(&inbound, process_func, instance)?;
                    self.get_outbound(
                        component_key,
                        instance,
                        process_func,
                        outbound_msgs,
                        results,
                    )?;
                }
            }
        }
        Ok(())
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
        for msg in inbound {
            match msg {
                InboundComponentMsg::ApplicationMessage(ApplicationMessage {
                    app,
                    payload,
                    context,
                    ..
                }) => {
                    let outbound = self.exec_inbound(
                        &InboundComponentMsg::ApplicationMessage(ApplicationMessage::new(
                            app, payload, context,
                        )),
                        &process_func,
                        &instance,
                    )?;
                    self.get_outbound(key, &instance, &process_func, outbound, &mut results)?;
                }
                InboundComponentMsg::UserResponse(response) => {
                    let outbound = self.exec_inbound(
                        &InboundComponentMsg::UserResponse(response),
                        &process_func,
                        &instance,
                    )?;
                    self.get_outbound(key, &instance, &process_func, outbound, &mut results)?;
                }
                InboundComponentMsg::GetSecretResponse(_) => {
                    return Err(ComponentExecError::UnexpectedMessage("get secret response").into())
                }
                InboundComponentMsg::RandomBytes(bytes) => {
                    let outbound = self.exec_inbound(
                        &InboundComponentMsg::RandomBytes(bytes),
                        &process_func,
                        &instance,
                    )?;
                    self.get_outbound(key, &instance, &process_func, outbound, &mut results)?;
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
    use locutus_stdlib::{
        contract_interface::ContractCode,
        prelude::{env_logger, ComponentContext, ContractInstanceId, Parameters},
    };
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
            "api-test-{}",
            TEST_NO.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        ));
        if !test_dir.exists() {
            std::fs::create_dir_all(&test_dir).unwrap();
        }
        test_dir
    }

    fn get_test_component(name: &str) -> Component {
        const CONTRACTS_DIR: &str = env!("CARGO_MANIFEST_DIR");
        let contracts = PathBuf::from(CONTRACTS_DIR);
        let mut dirs = contracts.ancestors();
        let path = dirs.nth(2).unwrap();
        let component_path = path
            .join("tests")
            .join(name.replace('_', "-"))
            .join("build/locutus")
            .join(name)
            .with_extension("wasm");
        Component::try_from(component_path.as_path()).unwrap()
    }

    fn set_up_runtime(name: &str) -> (Component, Runtime) {
        let _ = env_logger::try_init();
        let contracts_dir = test_dir();
        let components_dir = test_dir();
        // let secrets_dir = test_dir();

        let contract_store = ContractStore::new(contracts_dir, 10_000).unwrap();
        let component_store = ComponentStore::new(components_dir.clone(), 10_000).unwrap();
        let secret_store = SecretsStore::from_dir(components_dir);

        let mut runtime =
            Runtime::build(contract_store, component_store, secret_store, false).unwrap();

        let component = get_test_component(name);
        let _ = runtime.component_store.store_component(component.clone());

        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        let cipher = XChaCha20Poly1305::new(&key);
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let _ = runtime
            .secret_store
            .register_component(component.key().clone(), cipher, nonce);

        runtime.enable_wasi = true; // ENABLE FOR DEBUGGING; requires building for wasi
        (component, runtime)
    }

    #[test]
    fn validate_process() -> Result<(), Box<dyn std::error::Error>> {
        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1])),
            Parameters::from(vec![]),
        );

        let (component, mut runtime) = set_up_runtime(TEST_COMPONENT_1);

        let payload: Vec<u8> = serde_json::to_vec(&InboundAppMessage::CreateInboxRequest).unwrap();
        let app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();
        let context = ComponentContext::new(vec![]);
        let msg: ApplicationMessage = ApplicationMessage::new(app, payload, context);

        let inbound_app_msg = InboundComponentMsg::ApplicationMessage(msg);
        let outbound = runtime.inbound_app_message(component.key(), vec![inbound_app_msg]);
        println!("{:?}", outbound);
        Ok(())
    }
}
