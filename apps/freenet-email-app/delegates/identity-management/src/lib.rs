use locutus_stdlib::prelude::{DelegateError, DelegateInterface, InboundDelegateMsg, OutboundDelegateMsg, Parameters};

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