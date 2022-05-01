use locutus_stdlib::prelude::*;

struct Contract;

#[contract]
impl ContractInterface for Contract {
    fn validate_state(_parameters: Parameters<'static>, state: State<'static>) -> bool {
        // let state_bytes = state.as_ref();
        // eprintln!("state: {state_bytes:?}");
        state[0] == 1 && state[3] == 4
    }

    fn validate_delta(_parameters: Parameters<'static>, delta: StateDelta<'static>) -> bool {
        delta[0] == 1 && delta[3] == 4
    }

    fn update_state(
        _parameters: Parameters<'static>,
        mut state: State<'static>,
        delta: StateDelta<'static>,
    ) -> Result<UpdateModification, ContractError> {
        let new_state = state.to_mut();
        new_state.extend(delta.as_ref());
        Ok(UpdateModification::ValidUpdate(State::from(
            new_state.to_vec(),
        )))
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
    ) -> StateSummary<'static> {
        let state = state.as_ref();
        // eprintln!("state: {state:?}");
        // eprintln!("summary: {:?}", &state[0..1]);
        StateSummary::from(state[0..1].to_vec())
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> StateDelta<'static> {
        let state = state.as_ref();
        assert!(state.len() == 4);
        let summary = summary.as_ref();
        assert!(summary.len() == 2);
        assert!(&state[1..=2] == summary);
        StateDelta::from((&state[3..=3]).to_vec())
    }

    fn update_state_from_summary(
        _parameters: Parameters<'static>,
        mut state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<UpdateModification, ContractError> {
        let new_state = state.to_mut();
        new_state.extend(summary.as_ref());
        Ok(UpdateModification::ValidUpdate(State::from(
            new_state.to_vec(),
        )))
    }
}
