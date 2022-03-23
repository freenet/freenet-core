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
    let summarize_fn = s.gen_summarize_state_fn();
    let get_delta_fn = s.gen_get_state_delta();
    let from_summary = s.gen_update_from_summary_fn();
    quote! {
        #validate_state_func
        #validate_delta_func
        #update_func
        #summarize_fn
        #get_delta_fn
        #from_summary
    }
}

struct ImplStruct {
    type_name: TypePath,
}

impl ImplStruct {
    fn gen_validate_state(&self) -> TokenStream {
        let type_name = &self.type_name;
        quote! {
            #[no_mangle]
            pub fn validate_state(parameters: i64, state: i64) -> i32 {
                let parameters = unsafe {
                    // eprintln!("getting params: {:p}", state as *mut BufferBuilder);
                    let param_buf = &*(parameters as *const BufferBuilder);
                    let bytes = &*std::ptr::slice_from_raw_parts(
                        param_buf.start as *const u8,
                        param_buf.size as usize,
                    );
                    Parameters::from(bytes)
                };
                let state = unsafe {
                    // eprintln!("getting state: {:p}", state as *mut BufferBuilder);
                    let state_buf = &*(state as *const BufferBuilder);
                    let bytes = &*std::ptr::slice_from_raw_parts(
                        state_buf.start as *const u8,
                        state_buf.size as usize,
                    );
                    State::from(bytes)
                };
                let result = <#type_name as ::locutus_stdlib::prelude::ContractInterface>::validate_state(parameters, state);
                result as i32
            }
        }
    }

    fn gen_validate_delta(&self) -> TokenStream {
        let type_name = &self.type_name;
        quote! {
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
                let result = <#type_name as ::locutus_stdlib::prelude::ContractInterface>::validate_delta(parameters, delta);
                result as i32
            }
        }
    }

    fn gen_update_fn(&self) -> TokenStream {
        let type_name = &self.type_name;
        quote! {
            #[no_mangle]
            pub fn update_state(parameters: i64, state: i64, delta: i64) -> i32 {
                let parameters = unsafe {
                    let param_buf = Box::from_raw(parameters as *mut ::locutus_stdlib::prelude::BufferBuilder);
                    let bytes =
                    &*std::ptr::slice_from_raw_parts(param_buf.start as *const u8, param_buf.size as usize);
                    ::locutus_stdlib::prelude::Parameters::from(bytes)
                };
                let state = unsafe {
                    let state_buf = Box::from_raw(state as *mut ::locutus_stdlib::prelude::BufferBuilder);
                    let bytes =
                    &*std::ptr::slice_from_raw_parts(state_buf.start as *const u8, state_buf.size as usize);
                    ::locutus_stdlib::prelude::State::from(bytes)
                };
                let delta = unsafe {
                    let delta_buf = Box::from_raw(delta as *mut ::locutus_stdlib::prelude::BufferBuilder);
                    let bytes =
                    &*std::ptr::slice_from_raw_parts(delta_buf.start as *const u8, delta_buf.size as usize);
                    ::locutus_stdlib::prelude::StateDelta::from(bytes)
                };
                let result = <#type_name as ::locutus_stdlib::prelude::ContractInterface>::update_state(parameters, state, delta);
                result as i32
            }
        }
    }

    fn gen_summarize_state_fn(&self) -> TokenStream {
        let type_name = &self.type_name;
        quote! {
            #[no_mangle]
            pub fn summarize_state(parameters: i64, state: i64) -> i64 {
                let parameters = unsafe {
                    let param_buf = Box::from_raw(parameters as *mut ::locutus_stdlib::prelude::BufferBuilder);
                    let bytes =
                    &*std::ptr::slice_from_raw_parts(param_buf.start as *const u8, param_buf.size as usize);
                    ::locutus_stdlib::prelude::Parameters::from(bytes)
                };
                let state = unsafe {
                    let state_buf = Box::from_raw(state as *mut ::locutus_stdlib::prelude::BufferBuilder);
                    let bytes =
                    &*std::ptr::slice_from_raw_parts(state_buf.start as *const u8, state_buf.size as usize);
                    ::locutus_stdlib::prelude::State::from(bytes)
                };
                let summary = <#type_name as ::locutus_stdlib::prelude::ContractInterface>::summarize_state(parameters, state);
                ::locutus_stdlib::buffer::initiate_buffer(summary.size() as u32, false as i32)
            }
        }
    }

    fn gen_get_state_delta(&self) -> TokenStream {
        let type_name = &self.type_name;
        quote! {
            #[no_mangle]
            pub fn get_state_delta(parameters: i64, state: i64, summary: i64) -> i64 {
                let parameters = unsafe {
                    let param_buf = Box::from_raw(parameters as *mut ::locutus_stdlib::prelude::BufferBuilder);
                    let bytes =
                    &*std::ptr::slice_from_raw_parts(param_buf.start as *const u8, param_buf.size as usize);
                    ::locutus_stdlib::prelude::Parameters::from(bytes)
                };
                let state = unsafe {
                    let state_buf = Box::from_raw(state as *mut ::locutus_stdlib::prelude::BufferBuilder);
                    let bytes =
                    &*std::ptr::slice_from_raw_parts(state_buf.start as *const u8, state_buf.size as usize);
                    ::locutus_stdlib::prelude::State::from(bytes)
                };
                let summary = unsafe {
                    let summary_buf = Box::from_raw(summary as *mut ::locutus_stdlib::prelude::BufferBuilder);
                    let bytes =
                    &*std::ptr::slice_from_raw_parts(summary_buf.start as *const u8, summary_buf.size as usize);
                    ::locutus_stdlib::prelude::StateSummary::from(bytes)
                };
                let new_delta = <#type_name as ::locutus_stdlib::prelude::ContractInterface>::get_state_delta(parameters, state, summary);
                ::locutus_stdlib::buffer::initiate_buffer(new_delta.size() as u32, false as i32)
            }
        }
    }

    fn gen_update_from_summary_fn(&self) -> TokenStream {
        let type_name = &self.type_name;
        quote! {
            #[no_mangle]
            pub fn update_state_from_summary(parameters: i64, current_state: i64, current_summary: i64) -> i32 {
                let parameters = unsafe {
                    let param_buf = Box::from_raw(parameters as *mut ::locutus_stdlib::prelude::BufferBuilder);
                    let bytes =
                    &*std::ptr::slice_from_raw_parts(param_buf.start as *const u8, param_buf.size as usize);
                    ::locutus_stdlib::prelude::Parameters::from(bytes)
                };
                let state = unsafe {
                    let state_buf = Box::from_raw(current_state as *mut ::locutus_stdlib::prelude::BufferBuilder);
                    let bytes =
                    &*std::ptr::slice_from_raw_parts(state_buf.start as *const u8, state_buf.size as usize);
                    ::locutus_stdlib::prelude::State::from(bytes)
                };
                let summary = unsafe {
                    let summary_buf = Box::from_raw(current_summary as *mut ::locutus_stdlib::prelude::BufferBuilder);
                    let bytes =
                    &*std::ptr::slice_from_raw_parts(summary_buf.start as *const u8, summary_buf.size as usize);
                    ::locutus_stdlib::prelude::StateSummary::from(bytes)
                };
                let result = <#type_name as ::locutus_stdlib::prelude::ContractInterface>::update_state_from_summary(parameters, state, summary);
                result as i32
            }
        }
    }
}
