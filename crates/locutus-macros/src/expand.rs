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
    let update_func = s.gen_update_state_fn();
    let summarize_fn = s.gen_summarize_state_fn();
    let get_delta_fn = s.gen_get_state_delta();
    let result = quote! {
        #validate_state_func
        #validate_delta_func
        #update_func
        #summarize_fn
        #get_delta_fn
    };
    // println!("{result}");
    result
}

fn ffi_ret_type() -> TokenStream {
    quote!((i64, i32, u32))
}

struct ImplStruct {
    type_name: TypePath,
}

impl ImplStruct {
    fn gen_validate_state(&self) -> TokenStream {
        let type_name = &self.type_name;
        let ret = ffi_ret_type();
        quote! {
            #[no_mangle]
            pub fn validate_state(parameters: i64, state: i64, related: i64) -> #ret {
                let parameters = unsafe {
                    // eprintln!("getting params: {:p}", state as *mut ::locutus_stdlib::buf::BufferBuilder);
                    let param_buf = &*(parameters as *const ::locutus_stdlib::buf::BufferBuilder);
                    let bytes = &*std::ptr::slice_from_raw_parts(
                        param_buf.start(),
                        param_buf.len(),
                    );
                    Parameters::from(bytes)
                };
                let state = unsafe {
                    // eprintln!("getting state: {:p}", state as *mut ::locutus_stdlib::buf::BufferBuilder);
                    let state_buf = &*(state as *const ::locutus_stdlib::buf::BufferBuilder);
                    let bytes = &*std::ptr::slice_from_raw_parts(
                        state_buf.start(),
                        state_buf.len(),
                    );
                    State::from(bytes)
                };
                let related: ::locutus_stdlib::prelude::RelatedContracts = unsafe {
                    let related = &*(related as *const ::locutus_stdlib::buf::BufferBuilder);
                    let bytes = &*std::ptr::slice_from_raw_parts(related.start(), related.len());
                    ::locutus_stdlib::prelude::bincode::deserialize(bytes)
                        .expect("correctly serialized `RelatedContract`")
                };
                let result = <#type_name as ::locutus_stdlib::prelude::ContractInterface>::validate_state(parameters, state, related);
                ::locutus_stdlib::prelude::InterfaceResult::from(result).into_raw()
            }
        }
    }

    fn gen_validate_delta(&self) -> TokenStream {
        let type_name = &self.type_name;
        let ret = ffi_ret_type();
        quote! {
            #[no_mangle]
            pub fn validate_delta(parameters: i64, delta: i64) -> #ret {
                let parameters = unsafe {
                    let param_buf = &mut *(parameters as *mut ::locutus_stdlib::buf::BufferBuilder);
                    let bytes =
                        &*std::ptr::slice_from_raw_parts(param_buf.start(), param_buf.len());
                    Parameters::from(bytes)
                };
                let delta = unsafe {
                    let delta_buf = &mut *(delta as *mut ::locutus_stdlib::buf::BufferBuilder);
                    let bytes =
                        &*std::ptr::slice_from_raw_parts(delta_buf.start(), delta_buf.len());
                    StateDelta::from(bytes)
                };
                let result = <#type_name as ::locutus_stdlib::prelude::ContractInterface>::validate_delta(parameters, delta);
                ::locutus_stdlib::prelude::InterfaceResult::from(result).into_raw()
            }
        }
    }

    fn gen_update_state_fn(&self) -> TokenStream {
        let type_name = &self.type_name;
        let ret = ffi_ret_type();
        quote! {
            #[no_mangle]
            pub fn update_state(parameters: i64, state: i64, delta: i64) -> #ret {
                let parameters = unsafe {
                    let param_buf = &mut *(parameters as *mut ::locutus_stdlib::buf::BufferBuilder);
                    let bytes =
                        &*std::ptr::slice_from_raw_parts(param_buf.start(), param_buf.len());
                    ::locutus_stdlib::prelude::Parameters::from(bytes)
                };
                let (state, mut result_buf) = unsafe {
                    let state_buf = &mut *(state as *mut ::locutus_stdlib::buf::BufferBuilder);
                    let bytes =
                        &*std::ptr::slice_from_raw_parts(state_buf.start(), state_buf.len());
                    (::locutus_stdlib::prelude::State::from(bytes), state_buf)
                };
                let updates = unsafe {
                    let delta_buf = &mut *(delta as *mut ::locutus_stdlib::buf::BufferBuilder);
                    let bytes = &*std::ptr::slice_from_raw_parts(delta_buf.start(), delta_buf.len());
                    bincode::deserialize(bytes).unwrap()
                };
                let result = <#type_name as ::locutus_stdlib::prelude::ContractInterface>::update_state(
                    parameters, state, updates,
                );
                ::locutus_stdlib::prelude::InterfaceResult::from(result).into_raw()
            }
        }
    }

    fn gen_summarize_state_fn(&self) -> TokenStream {
        let type_name = &self.type_name;
        let ret = ffi_ret_type();
        quote! {
            #[no_mangle]
            pub fn summarize_state(parameters: i64, state: i64) -> #ret {
                let parameters = unsafe {
                    let param_buf = &mut *(parameters as *mut ::locutus_stdlib::buf::BufferBuilder);
                    let bytes = &*std::ptr::slice_from_raw_parts(param_buf.start(), param_buf.len());
                    ::locutus_stdlib::prelude::Parameters::from(bytes)
                };
                let state = unsafe {
                    let state_buf = &mut *(state as *mut ::locutus_stdlib::buf::BufferBuilder);
                    let bytes = &*std::ptr::slice_from_raw_parts(state_buf.start(), state_buf.len());
                    ::locutus_stdlib::prelude::State::from(bytes)
                };
                let summary = <#type_name as ::locutus_stdlib::prelude::ContractInterface>::summarize_state(parameters, state);
                ::locutus_stdlib::prelude::InterfaceResult::from(summary).into_raw()
            }
        }
    }

    fn gen_get_state_delta(&self) -> TokenStream {
        let type_name = &self.type_name;
        let ret = ffi_ret_type();
        quote! {
            #[no_mangle]
            pub fn get_state_delta(parameters: i64, state: i64, summary: i64) -> #ret {
                let parameters = unsafe {
                    let param_buf = &mut *(parameters as *mut ::locutus_stdlib::buf::BufferBuilder);
                    let bytes =
                    &*std::ptr::slice_from_raw_parts(param_buf.start(), param_buf.len());
                    ::locutus_stdlib::prelude::Parameters::from(bytes)
                };
                let state = unsafe {
                    let state_buf = &mut *(state as *mut ::locutus_stdlib::buf::BufferBuilder);
                    let bytes =
                    &*std::ptr::slice_from_raw_parts(state_buf.start(), state_buf.len());
                    ::locutus_stdlib::prelude::State::from(bytes)
                };
                let summary = unsafe {
                    let summary_buf = &mut *(summary as *mut ::locutus_stdlib::buf::BufferBuilder);
                    let bytes =
                    &*std::ptr::slice_from_raw_parts(summary_buf.start(), summary_buf.len());
                    ::locutus_stdlib::prelude::StateSummary::from(bytes)
                };
                let new_delta = <#type_name as ::locutus_stdlib::prelude::ContractInterface>::get_state_delta(parameters, state, summary);
                ::locutus_stdlib::prelude::InterfaceResult::from(new_delta).into_raw()
            }
        }
    }
}
