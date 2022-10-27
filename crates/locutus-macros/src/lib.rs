use quote::quote;
use syn::{AttributeArgs, ItemImpl};

mod component_impl;
mod contract_impl;

/// Generate the necessary code for the WASM runtime to interact with your contract ergonomically and safely.
#[proc_macro_attribute]
pub fn contract(
    args: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let _args = syn::parse_macro_input!(args as AttributeArgs);
    let input = syn::parse_macro_input!(input as ItemImpl);
    let output = contract_impl::ffi_impl_wrap(&input);
    // println!("{}", quote!(#input));
    // println!("{output}");
    proc_macro::TokenStream::from(quote! {
        #input
        #output
    })
}

/// Generate the necessary code for the WASM runtime to interact with your contract ergonomically and safely.
#[proc_macro_attribute]
pub fn component(
    args: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let _args = syn::parse_macro_input!(args as AttributeArgs);
    let input = syn::parse_macro_input!(input as ItemImpl);
    let output = component_impl::ffi_impl_wrap(&input);
    // println!("{}", quote!(#input));
    // println!("{output}");
    proc_macro::TokenStream::from(quote! {
        #input
        #output
    })
}
