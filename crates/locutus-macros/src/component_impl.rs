use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemImpl;

pub fn ffi_impl_wrap(item: &ItemImpl) -> TokenStream {
    quote!()
}
