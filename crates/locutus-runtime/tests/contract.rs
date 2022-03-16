use locutus_runtime::*;

struct Contract;

#[locutus_macros::contract]
impl ContractInterface for Contract {
    fn validate_state(parameters: Parameters<'static>, state: State<'static>) -> bool {
        todo!()
    }

    fn validate_delta(parameters: Parameters<'static>, delta: StateDelta<'static>) -> bool {
        todo!()
    }

    fn update_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        delta: StateDelta<'static>,
    ) -> UpdateResult {
        todo!()
    }
}
