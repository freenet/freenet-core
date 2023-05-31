use locutus_stdlib::prelude::*;

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
