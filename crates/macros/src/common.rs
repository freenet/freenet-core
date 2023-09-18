use proc_macro2::TokenStream;
use quote::quote;

pub fn set_logger() -> TokenStream {
    // TODO: add log level as a parameter to the macro
    quote! {
        #[cfg(feature = "trace")]
        {
            use ::freenet_stdlib::prelude::{tracing_subscriber as tra};
            if let Err(err) = tra::fmt()
                .with_env_filter("warn,freenet_stdlib=trace")
                .try_init()
            {
                return ::freenet_stdlib::prelude::ContractInterfaceResult::from(
                    Err::<::freenet_stdlib::prelude::ValidateResult, _>(
                        ::freenet_stdlib::prelude::ContractError::Other(format!("{}", err))
                    )
                ).into_raw();
            }
        }
    }
}
