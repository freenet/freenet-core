use locutus_stdlib::prelude::*;

struct AliasAllocationDelegate;

#[derive(Debug, Serialize, Deserialize, Default)]
struct AliasesContext {
    alias_to_key: HashMap<String, String>,
    key_to_alias: HashMap<String, String>,
}

#[delegate]
impl DelegateInterface for AliasAllocationDelegate {
    fn process(
        _params: Parameters<'static>,
        _message: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        todo!()
    }
}
