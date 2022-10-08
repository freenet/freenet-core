use quote::quote;
use syn::{AttributeArgs, ItemImpl};

mod expand;

#[proc_macro_attribute]
pub fn contract(
    args: proc_macro::TokenStream,
    input: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let _args = syn::parse_macro_input!(args as AttributeArgs);
    let input = syn::parse_macro_input!(input as ItemImpl);
    let output = expand::ffi_impl_wrap(&input);
    // println!("{}", quote!(#input));
    // println!("{output}");
    proc_macro::TokenStream::from(quote! {
        #input
        #output
    })
}
