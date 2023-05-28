use locutus_stdlib::prelude::{DelegateError, DelegateInterface, InboundDelegateMsg, OutboundDelegateMsg, Parameters};

struct AliasAllocationDelegate;

#[delegate]
impl DelegateInterface for AliasAllocationDelegate {
    fn process(
        _params: Parameters<'static>,
        _message: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        todo!()
    }
}