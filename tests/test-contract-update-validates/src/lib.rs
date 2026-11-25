// Test contract for the `__frnt__update_validates` declaration.
//
// With the `declares-update-validates` feature (the default of the
// `test-contract-update-validates` crate) it exports the marker; the
// `test-contract-update-validates-off` crate includes this same source without
// the feature, so the two builds differ in nothing but the export.
//
// Behaviour, chosen so a test can tell from outside whether `validate_state`
// ran:
// - `validate_state`: Invalid for a state whose first byte is 0xFF, else Valid.
// - `update_state`: refuses an incoming state or delta whose first byte is
//   0xEE; otherwise the last incoming state or delta wins.
// - `summarize_state` / `get_state_delta`: as the mock-aligned test contract.

use freenet_stdlib::prelude::*;

struct Contract;

#[cfg(feature = "declares-update-validates")]
#[unsafe(no_mangle)]
pub extern "C" fn __frnt__update_validates() -> i32 {
    1
}

#[contract]
impl ContractInterface for Contract {
    fn validate_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        _related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError> {
        if state.as_ref().first() == Some(&0xFF) {
            Ok(ValidateResult::Invalid)
        } else {
            Ok(ValidateResult::Valid)
        }
    }

    fn update_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
        data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        let mut new_state = None;
        for ud in &data {
            let bytes = match ud {
                UpdateData::State(s) => Some(s.as_ref().to_vec()),
                UpdateData::Delta(d) => Some(d.as_ref().to_vec()),
                _ => None,
            };
            if let Some(bytes) = bytes {
                if bytes.first() == Some(&0xEE) {
                    return Err(ContractError::InvalidUpdate);
                }
                new_state = Some(State::from(bytes));
            }
        }
        match new_state {
            Some(s) => Ok(UpdateModification::valid(s)),
            None => Ok(UpdateModification::valid(state)),
        }
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
        Ok(StateSummary::from(
            blake3::hash(state.as_ref()).as_bytes().to_vec(),
        ))
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        state: State<'static>,
        _summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        Ok(StateDelta::from(state.as_ref().to_vec()))
    }
}
