//! This contract just checks that macros compile etc.
// ANCHOR: componentifce
use locutus_stdlib::prelude::*;

pub const RANDOM_SIGNATURE: &[u8] = &[6, 8, 2, 5, 6, 9, 9, 10];

struct Delegate;

#[component]
impl DelegateInterface for Delegate {
    fn process(
        _messages: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        unimplemented!()
    }
}
// ANCHOR_END: componentifce

fn main() {}
