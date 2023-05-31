use locutus_stdlib::prelude::*;

struct IdentityManagementDelegate;

#[delegate]
impl DelegateInterface for IdentityManagementDelegate {
    fn process(
        _params: Parameters<'static>,
        _message: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        todo!()
    }
}
