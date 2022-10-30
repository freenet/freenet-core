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

    fn set_logger(&self) -> TokenStream {
        // TODO: add log level as a parameter to the macro
        quote! {
            #[cfg(feature = "trace")]
            {
                use ::locutus_stdlib::prelude::log;
                if let Err(err) = ::locutus_stdlib::prelude::env_logger::builder()
                    .filter_level(log::LevelFilter::Info)
                    .filter_module("locutus_stdlib", log::LevelFilter::Trace)
                    .try_init()
                {
                    return ::locutus_stdlib::prelude::ContractInterfaceResult::from(
                        Err::<::locutus_stdlib::prelude::ValidateResult, _>(
                            ::locutus_stdlib::prelude::ContractError::Other(format!("{}", err))
                        )
                    ).into_raw();
                }
            }
        }
    }

    fn gen_process_fn(&self) -> TokenStream {
        let type_name = &self.type_name;
        let ret = self.ffi_ret_type();
        let set_logger = self.set_logger();
        quote! {
            #[no_mangle]
            pub extern "C" fn process(inbound: i64) -> #ret {
                #set_logger
                let inbound = unsafe {
                    let inbound_buf = &mut *(inbound as *mut ::locutus_stdlib::buf::BufferBuilder);
                    let bytes =
                        &*std::ptr::slice_from_raw_parts(inbound_buf.start(), inbound_buf.written(None));
                    match ::locutus_stdlib::prelude::bincode::deserialize(bytes) {
                        Ok(v) => v,
                        Err(err) => return ::locutus_stdlib::prelude::ComponentInterfaceResult::from(
                            Err::<::std::vec::Vec<::locutus_stdlib::prelude::OutboundComponentMsg>, _>(::locutus_stdlib::prelude::ComponentError::Deser(format!("{}", err)))
                        ).into_raw(),
                    }
                };
                let result =<#type_name as ::locutus_stdlib::prelude::ComponentInterface>::process(
                   inbound
                );
                ::locutus_stdlib::prelude::ComponentInterfaceResult::from(result).into_raw()
            }
        }
    }
}
