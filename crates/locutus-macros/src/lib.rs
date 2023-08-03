use quote::quote;
use syn::punctuated::Punctuated;
use syn::{ItemImpl, Meta, Token};

pub(crate) mod common;
mod contract_impl;
mod delegate_impl;

#[allow(dead_code)]
struct AttributeArgs {
    args: Punctuated<Meta, Token![,]>,
}

impl syn::parse::Parse for AttributeArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut args = Punctuated::new();
        while !input.is_empty() {
            let meta = input.parse::<Meta>()?;
            args.push(meta);
        }
        Ok(AttributeArgs { args })
    }
}

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
pub fn delegate(
    args: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let _args = syn::parse_macro_input!(args as AttributeArgs);
    let input = syn::parse_macro_input!(input as ItemImpl);
    let output = delegate_impl::ffi_impl_wrap(&input);
    // println!("{}", quote!(#input));
    // println!("{output}");
    proc_macro::TokenStream::from(quote! {
        #input
        #output
    })
}
