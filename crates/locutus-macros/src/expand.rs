use proc_macro2::TokenStream;
use quote::quote;
use syn::{ItemImpl, Type, TypePath};

pub fn ffi_impl_wrap(item: &ItemImpl) -> TokenStream {
    let type_name = match &*item.self_ty {
        Type::Path(p) => p.clone(),
        _ => panic!(),
    };
    let s = ImplStruct { type_name };
    let validate_state_func = s.gen_validate_state();
    let validate_delta_func = s.gen_validate_delta();
    let update_func = s.gen_update_fn();
    quote! {
        #validate_state_func
    #validate_delta_func
        #update_func
    }
}

struct ImplStruct {
    type_name: TypePath,
}

impl ImplStruct {
    fn gen_validate_state(&self) -> TokenStream {
        let type_name = &self.type_name;
        let validate_state = quote! {
            #[no_mangle]
            pub fn validate_state(parameters: i64, state: i64) -> i32 {
                let parameters = unsafe {
                    let param_buf = Box::from_raw(parameters as *mut BufferBuilder);
                    let bytes = &*std::ptr::slice_from_raw_parts(
                        param_buf.start as *const u8,
                        param_buf.size as usize,
                    );
                    Parameters::from(bytes)
                };
                let state = unsafe {
                    let state_buf = Box::from_raw(state as *mut BufferBuilder);
                    let bytes = &*std::ptr::slice_from_raw_parts(
                        state_buf.start as *const u8,
                        state_buf.size as usize,
                    );
                    State::from(bytes)
                };
                let result = <#type_name as ::locutus_runtime::ContractInterface>::validate_state(parameters, state);
                result as i32
            }
        };
        validate_state
    }

    fn gen_validate_delta(&self) -> TokenStream {
        let type_name = &self.type_name;
        let validate_delta = quote! {
            #[no_mangle]
            pub fn validate_delta(parameters: i64, delta: i64) -> i32 {
                let parameters = unsafe {
                    let param_buf = Box::from_raw(parameters as *mut BufferBuilder);
                    let bytes =
                        &*std::ptr::slice_from_raw_parts(param_buf.start as *const u8, param_buf.size as usize);
                    Parameters::from(bytes)
                };
                let delta = unsafe {
                    let delta_buf = Box::from_raw(delta as *mut BufferBuilder);
                    let bytes =
                        &*std::ptr::slice_from_raw_parts(delta_buf.start as *const u8, delta_buf.size as usize);
                    StateDelta::from(bytes)
                };
                let result = <#type_name as ::locutus_runtime::ContractInterface>::validate_delta(parameters, delta);
                result as i32
            }
        };
        validate_delta
    }

    fn gen_update_fn(&self) -> TokenStream {
        let type_name = &self.type_name;
        let update_state_fn = quote! {
            #[no_mangle]
            pub fn update_state(parameters: i64, state: i64, delta: i64) -> i32 {
                let parameters = unsafe {
                    let param_buf = Box::from_raw(parameters as *mut ::locutus_runtime::BufferBuilder);
                    let bytes =
                    &*std::ptr::slice_from_raw_parts(param_buf.start as *const u8, param_buf.size as usize);
                    ::locutus_runtime::Parameters::from(bytes)
                };
                let state = unsafe {
                    let state_buf = Box::from_raw(state as *mut ::locutus_runtime::BufferBuilder);
                    let bytes =
                    &*std::ptr::slice_from_raw_parts(state_buf.start as *const u8, state_buf.size as usize);
                    ::locutus_runtime::State::from(bytes)
                };
                let delta = unsafe {
                    let delta_buf = Box::from_raw(delta as *mut ::locutus_runtime::BufferBuilder);
                    let bytes =
                    &*std::ptr::slice_from_raw_parts(delta_buf.start as *const u8, delta_buf.size as usize);
                    ::locutus_runtime::StateDelta::from(bytes)
                };
                let result = <#type_name as ::locutus_runtime::ContractInterface>::update_state(parameters, state, delta);
                result as i32
            }
        };
        update_state_fn
    }
}
