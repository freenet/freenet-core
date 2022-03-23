use locutus_stdlib::prelude::*;

struct Contract;

#[contract]
impl ContractInterface for Contract {
    fn validate_state(parameters: Parameters<'static>, state: State<'static>) -> bool {
        let state_bytes = state.as_ref();
        // eprintln!("state: {state_bytes:?}");
        state[0] == 1 && state[3] == 4
    }
    fn validate_delta(_parameters: Parameters<'static>, _delta: StateDelta<'static>) -> bool {
        unimplemented!()
    }

    fn update_state(
        _parameters: Parameters<'static>,
        _state: State<'static>,
        _delta: StateDelta<'static>,
    ) -> UpdateResult {
        unimplemented!()
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
    ) -> StateSummary<'static> {
        let state = state.as_ref();
        StateSummary::from(state[0..3].to_vec())
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        _state: State<'static>,
        _summary: StateSummary<'static>,
    ) -> StateDelta<'static> {
        unimplemented!()
    }

    fn update_state_from_summary(
        _parameters: Parameters<'static>,
        _state: State<'static>,
        _summary: StateSummary<'static>,
    ) -> UpdateResult {
        unimplemented!()
    }
}
