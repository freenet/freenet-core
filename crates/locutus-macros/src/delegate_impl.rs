use proc_macro2::TokenStream;
use quote::quote;
use syn::{ItemImpl, Type, TypePath};

pub fn ffi_impl_wrap(item: &ItemImpl) -> TokenStream {
    let type_name = match &*item.self_ty {
        Type::Path(p) => p.clone(),
        _ => panic!(),
    };
    let s = ImplStruct { type_name };
    let process_fn = s.gen_process_fn();
    quote!(#process_fn)
}

struct ImplStruct {
    type_name: TypePath,
}

impl ImplStruct {
    fn ffi_ret_type(&self) -> TokenStream {
        quote!(i64)
    }

    fn gen_process_fn(&self) -> TokenStream {
        let type_name = &self.type_name;
        let ret = self.ffi_ret_type();
        let set_logger = crate::common::set_logger();
        quote! {
            #[no_mangle]
            pub extern "C" fn process(parameters: i64, inbound: i64) -> #ret {
                #set_logger
                let parameters = unsafe {
                    let param_buf = &*(parameters as *const ::locutus_stdlib::buf::BufferBuilder);
                    let bytes = &*std::ptr::slice_from_raw_parts(
                        param_buf.start(),
                        param_buf.written(None),
                    );
                    Parameters::from(bytes)
                };
                let inbound = unsafe {
                    let inbound_buf = &mut *(inbound as *mut ::locutus_stdlib::buf::BufferBuilder);
                    let bytes =
                        &*std::ptr::slice_from_raw_parts(inbound_buf.start(), inbound_buf.written(None));
                    match ::locutus_stdlib::prelude::bincode::deserialize(bytes) {
                        Ok(v) => v,
                        Err(err) => return ::locutus_stdlib::prelude::DelegateInterfaceResult::from(
                            Err::<::std::vec::Vec<::locutus_stdlib::prelude::OutboundDelegateMsg>, _>(::locutus_stdlib::prelude::DelegateError::Deser(format!("{}", err)))
                        ).into_raw(),
                    }
                };
                let result =<#type_name as ::locutus_stdlib::prelude::DelegateInterface>::process(
                    parameters,
                    inbound
                );
                ::locutus_stdlib::prelude::DelegateInterfaceResult::from(result).into_raw()
            }
        }
    }
}
