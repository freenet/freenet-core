//! This contract just checks that macros compile etc.
// ANCHOR: componentifce
use locutus_stdlib::prelude::*;

pub const RANDOM_SIGNATURE: &[u8] = &[6, 8, 2, 5, 6, 9, 9, 10];

struct Component;

#[component]
impl ComponentInterface for Component {
    fn process(
        _messages: InboundComponentMsg,
    ) -> Result<Vec<OutboundComponentMsg>, ComponentError> {
        unimplemented!()
    }
}
// ANCHOR_END: componentifce

fn main() {}
