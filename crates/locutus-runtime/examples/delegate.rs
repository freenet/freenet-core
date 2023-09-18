//! This contract just checks that macros compile etc.
// ANCHOR: delegateifce
use freenet_stdlib::prelude::*;

pub const RANDOM_SIGNATURE: &[u8] = &[6, 8, 2, 5, 6, 9, 9, 10];

struct Delegate;

#[delegate]
impl DelegateInterface for Delegate {
    fn process(
        _parameters: Parameters<'static>,
        _attested: Option<&'static [u8]>,
        _messages: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        unimplemented!()
    }
}
// ANCHOR_END: delegateifce

fn main() {}
