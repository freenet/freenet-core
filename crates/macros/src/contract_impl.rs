use proc_macro2::TokenStream;
use quote::quote;
use syn::{ItemImpl, Type, TypePath};

pub fn ffi_impl_wrap(item: &ItemImpl) -> TokenStream {
    let type_name = match &*item.self_ty {
        Type::Path(p) => p.clone(),
        _ => panic!(),
    };
    let s = ImplStruct { type_name };
    let validate_state_fn = s.gen_validate_state_fn();
    let validate_delta_fn = s.gen_validate_delta_fn();
    let update_fn = s.gen_update_state_fn();
    let summarize_fn = s.gen_summarize_state_fn();
    let get_delta_fn = s.gen_get_state_delta();
    let result = quote! {
        #validate_state_fn
        #validate_delta_fn
        #update_fn
        #summarize_fn
        #get_delta_fn
    };
    // println!("{result}");
    result
}

struct ImplStruct {
    type_name: TypePath,
}

impl ImplStruct {
    fn ffi_ret_type(&self) -> TokenStream {
        quote!(i64)
    }

    fn gen_validate_state_fn(&self) -> TokenStream {
        let type_name = &self.type_name;
        let ret = self.ffi_ret_type();
        let set_logger = crate::common::set_logger();
        quote! {
            #[no_mangle]
            pub extern "C" fn validate_state(parameters: i64, state: i64, related: i64) -> #ret {
                #set_logger
                let parameters = unsafe {
                    let param_buf = &*(parameters as *const ::freenet_stdlib::buf::BufferBuilder);
                    let bytes = &*std::ptr::slice_from_raw_parts(
                        param_buf.start(),
                        param_buf.written(None),
                    );
                    Parameters::from(bytes)
                };
                let state = unsafe {
                    let state_buf = &*(state as *const ::freenet_stdlib::buf::BufferBuilder);
                    let bytes = &*std::ptr::slice_from_raw_parts(
                        state_buf.start(),
                        state_buf.written(None),
                    );
                    State::from(bytes)
                };
                let related: ::freenet_stdlib::prelude::RelatedContracts = unsafe {
                    let related = &*(related as *const ::freenet_stdlib::buf::BufferBuilder);
                    let bytes = &*std::ptr::slice_from_raw_parts(related.start(), related.written(None));
                    match ::freenet_stdlib::prelude::bincode::deserialize(bytes) {
                        Ok(v) => v,
                        Err(err) => return ::freenet_stdlib::prelude::ContractInterfaceResult::from(
                            Err::<::core::primitive::bool, _>(::freenet_stdlib::prelude::ContractError::Deser(format!("{}", err)))
                        ).into_raw(),
                    }
                };
                let result = <#type_name as ::freenet_stdlib::prelude::ContractInterface>::validate_state(parameters, state, related);
                ::freenet_stdlib::prelude::ContractInterfaceResult::from(result).into_raw()
            }
        }
    }

    fn gen_validate_delta_fn(&self) -> TokenStream {
        let type_name = &self.type_name;
        let ret = self.ffi_ret_type();
        let set_logger = crate::common::set_logger();
        quote! {
            #[no_mangle]
            pub extern "C" fn validate_delta(parameters: i64, delta: i64) -> #ret {
                #set_logger
                let parameters = unsafe {
                    let param_buf = &mut *(parameters as *mut ::freenet_stdlib::buf::BufferBuilder);
                    let bytes =
                        &*std::ptr::slice_from_raw_parts(param_buf.start(), param_buf.written(None));
                    Parameters::from(bytes)
                };
                let delta = unsafe {
                    let delta_buf = &mut *(delta as *mut ::freenet_stdlib::buf::BufferBuilder);
                    let bytes =
                        &*std::ptr::slice_from_raw_parts(delta_buf.start(), delta_buf.written(None));
                    StateDelta::from(bytes)
                };
                let result = <#type_name as ::freenet_stdlib::prelude::ContractInterface>::validate_delta(parameters, delta);
                ::freenet_stdlib::prelude::ContractInterfaceResult::from(result).into_raw()
            }
        }
    }

    fn gen_update_state_fn(&self) -> TokenStream {
        let type_name = &self.type_name;
        let ret = self.ffi_ret_type();
        let set_logger = crate::common::set_logger();
        quote! {
            #[no_mangle]
            pub extern "C" fn update_state(parameters: i64, state: i64, delta: i64) -> #ret {
                #set_logger
                let parameters = unsafe {
                    let param_buf = &mut *(parameters as *mut ::freenet_stdlib::buf::BufferBuilder);
                    let bytes =
                        &*std::ptr::slice_from_raw_parts(param_buf.start(), param_buf.written(None));
                    ::freenet_stdlib::prelude::Parameters::from(bytes)
                };
                let (state, mut result_buf) = unsafe {
                    let state_buf = &mut *(state as *mut ::freenet_stdlib::buf::BufferBuilder);
                    let bytes =
                        &*std::ptr::slice_from_raw_parts(state_buf.start(), state_buf.written(None));
                    (::freenet_stdlib::prelude::State::from(bytes), state_buf)
                };
                let updates = unsafe {
                    let updates = &mut *(delta as *mut ::freenet_stdlib::buf::BufferBuilder);
                    let bytes = &*std::ptr::slice_from_raw_parts(updates.start(), updates.written(None));
                    match ::freenet_stdlib::prelude::bincode::deserialize(bytes) {
                        Ok(v) => v,
                        Err(err) => return ::freenet_stdlib::prelude::ContractInterfaceResult::from(
                            Err::<::freenet_stdlib::prelude::ValidateResult, _>(
                                ::freenet_stdlib::prelude::ContractError::Deser(format!("{}", err))
                            )
                        ).into_raw(),
                    }
                };
                let result = <#type_name as ::freenet_stdlib::prelude::ContractInterface>::update_state(
                    parameters, state, updates,
                );
                ::freenet_stdlib::prelude::ContractInterfaceResult::from(result).into_raw()
            }
        }
    }

    fn gen_summarize_state_fn(&self) -> TokenStream {
        let type_name = &self.type_name;
        let ret = self.ffi_ret_type();
        let set_logger = crate::common::set_logger();
        quote! {
            #[no_mangle]
            pub extern "C" fn summarize_state(parameters: i64, state: i64) -> #ret {
                #set_logger
                let parameters = unsafe {
                    let param_buf = &mut *(parameters as *mut ::freenet_stdlib::buf::BufferBuilder);
                    let bytes = &*std::ptr::slice_from_raw_parts(param_buf.start(), param_buf.written(None));
                    ::freenet_stdlib::prelude::Parameters::from(bytes)
                };
                let state = unsafe {
                    let state_buf = &mut *(state as *mut ::freenet_stdlib::buf::BufferBuilder);
                    let bytes = &*std::ptr::slice_from_raw_parts(state_buf.start(), state_buf.written(None));
                    ::freenet_stdlib::prelude::State::from(bytes)
                };
                let summary = <#type_name as ::freenet_stdlib::prelude::ContractInterface>::summarize_state(parameters, state);
                ::freenet_stdlib::prelude::ContractInterfaceResult::from(summary).into_raw()
            }
        }
    }

    fn gen_get_state_delta(&self) -> TokenStream {
        let type_name = &self.type_name;
        let ret = self.ffi_ret_type();
        let set_logger = crate::common::set_logger();
        quote! {
            #[no_mangle]
            pub extern "C" fn get_state_delta(parameters: i64, state: i64, summary: i64) -> #ret {
                #set_logger
                let parameters = unsafe {
                    let param_buf = &mut *(parameters as *mut ::freenet_stdlib::buf::BufferBuilder);
                    let bytes =
                    &*std::ptr::slice_from_raw_parts(param_buf.start(), param_buf.written(None));
                    ::freenet_stdlib::prelude::Parameters::from(bytes)
                };
                let state = unsafe {
                    let state_buf = &mut *(state as *mut ::freenet_stdlib::buf::BufferBuilder);
                    let bytes =
                    &*std::ptr::slice_from_raw_parts(state_buf.start(), state_buf.written(None));
                    ::freenet_stdlib::prelude::State::from(bytes)
                };
                let summary = unsafe {
                    let summary_buf = &mut *(summary as *mut ::freenet_stdlib::buf::BufferBuilder);
                    let bytes =
                    &*std::ptr::slice_from_raw_parts(summary_buf.start(), summary_buf.written(None));
                    ::freenet_stdlib::prelude::StateSummary::from(bytes)
                };
                let new_delta = <#type_name as ::freenet_stdlib::prelude::ContractInterface>::get_state_delta(parameters, state, summary);
                ::freenet_stdlib::prelude::ContractInterfaceResult::from(new_delta).into_raw()
            }
        }
    }
}
