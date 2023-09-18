use proc_macro2::TokenStream;
use quote::quote;

pub fn set_logger() -> TokenStream {
    // TODO: add log level as a parameter to the macro
    quote! {
        #[cfg(feature = "trace")]
        {
            use ::locutus_stdlib::prelude::{tracing_subscriber as tra};
            if let Err(err) = tra::fmt()
                .with_env_filter("warn,locutus_stdlib=trace")
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
