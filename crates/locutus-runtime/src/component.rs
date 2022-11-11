use locutus_stdlib::prelude::{
    ApplicationMessage, Component, ComponentError, ComponentInterfaceResult, ComponentKey,
    GetSecretRequest, GetSecretResponse, InboundComponentMsg, OutboundComponentMsg,
    SetSecretRequest,
};
use wasmer::{Instance, NativeFunc};

use crate::{Runtime, RuntimeResult};

#[derive(thiserror::Error, Debug)]
pub enum ComponentExecError {
    #[error(transparent)]
    ComponentError(#[from] ComponentError),

    #[error("Received an unexpected message from the client apps: {0}")]
    UnexpectedMessage(&'static str),
}

pub trait ComponentRuntimeInterface {
    fn inbound_app_message<'a>(
        &mut self,
        key: &ComponentKey,
        inbound: Vec<InboundComponentMsg<'a>>,
    ) -> RuntimeResult<Vec<OutboundComponentMsg<'a>>>;

    fn register_component(&mut self, component: Component<'_>) -> RuntimeResult<()>;

    fn unregister_component(&mut self, key: &ComponentKey) -> RuntimeResult<()>;
}

impl Runtime {
    fn exec_inbound(
        &self,
        msg: &InboundComponentMsg,
        process_func: &NativeFunc<i64, i64>,
        instance: &Instance,
    ) -> RuntimeResult<Vec<OutboundComponentMsg<'static>>> {
        let msg = bincode::serialize(msg)?;
        let mut msg_buf = self.init_buf(instance, &msg)?;
        msg_buf.write(msg)?;
        let linear_mem = self.linear_mem(instance)?;
        let outbound = unsafe {
            ComponentInterfaceResult::from_raw(
                process_func.call(msg_buf.ptr() as i64)?,
                &linear_mem,
            )
            .unwrap(linear_mem)
            .map_err(Into::<ComponentExecError>::into)?
        };
        Ok(outbound)
    }

    fn get_outbound<'a>(
        &mut self,
        component_key: &ComponentKey,
        instance: &Instance,
        process_func: &NativeFunc<i64, i64>,
        outbound_msgs: Vec<OutboundComponentMsg<'a>>,
        results: &mut Vec<OutboundComponentMsg<'a>>,
    ) -> RuntimeResult<()> {
        let linear_mem = self.linear_mem(instance)?;
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
                    let msg = bincode::serialize(&inbound)?;
                    let mut msg_buf = self.init_buf(instance, &msg)?;
                    msg_buf.write(msg)?;
                    let outbound_msgs = unsafe {
                        ComponentInterfaceResult::from_raw(
                            process_func.call(msg_buf.ptr() as i64)?,
                            &linear_mem,
                        )
                        .unwrap(linear_mem)
                        .map_err(Into::<ComponentExecError>::into)?
                    };
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
            }
        }
        Ok(())
    }
}

impl ComponentRuntimeInterface for Runtime {
    fn inbound_app_message<'a>(
        &mut self,
        key: &ComponentKey,
        inbound: Vec<InboundComponentMsg<'a>>,
    ) -> RuntimeResult<Vec<OutboundComponentMsg<'a>>> {
        let mut results = Vec::with_capacity(inbound.len());
        if inbound.is_empty() {
            return Ok(results);
        }
        let instance = self.prepare_component_call(key, 4096)?;
        let process_func: NativeFunc<i64, i64> = instance.exports.get_native_function("process")?;
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
            }
        }
        Ok(results)
    }

    #[inline]
    fn register_component(&mut self, component: Component<'_>) -> RuntimeResult<()> {
        self.component_store.store_component(component)
    }

    #[inline]
    fn unregister_component(&mut self, key: &ComponentKey) -> RuntimeResult<()> {
        self.component_store.remove_component(key)
    }
}
