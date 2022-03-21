use locutus_stdlib::prelude::*;

struct Contract;

#[contract]
impl ContractInterface for Contract {
    fn validate_state(parameters: Parameters<'static>, state: State<'static>) -> bool {
        let state_bytes = state.as_ref();
        // eprintln!("state: {state_bytes:?}");
        state[0] == 1 && state[3] == 4
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
