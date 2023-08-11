use locutus_stdlib::prelude::*;

struct IdentityManagement;

#[delegate]
impl DelegateInterface for IdentityManagement {
    fn process(
        _params: Parameters<'static>,
        _message: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        todo!()
    }
}
