use locutus_stdlib::prelude::*;
use serde::{Serialize, Deserialize};
use locutus_stdlib::prelude::layer1::*;

// Don't get excited, not a cryptocurrency.

struct Wallet;

#[derive(Serialize, Deserialize)]
struct WalletState {
    pub balance : u64,
}

#[derive(Serialize, Deserialize)]
struct WalletSummary {
    pub balance : u64,
}

#[derive(Serialize, Deserialize)]
struct WalletParameters {
    pub owner_id : u64,
}

#[derive(Serialize, Deserialize)]
struct WalletDelta {
    pub balance : u64,
}

impl BincodeContract for Wallet { }

impl ContractAdapter for Wallet {
    type Parameters = WalletParameters;
    type State = WalletState;
    type StateSummary = WalletSummary;
    type StateDelta = WalletDelta;

    fn validate_state(
        parameters: Self::Parameters,
        state: Self::State,
        related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError> {
        Ok(ValidateResult::Valid)
    }

    fn validate_delta(
        parameters: Self::Parameters,
        delta: Self::StateDelta,
    ) -> Result<bool, ContractError> {
        Ok(true)
    }

    fn update_state(
        parameters: Self::Parameters,
        state: Self::State,
        data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        todo!()
    }

    fn summarize_state(
        parameters: Self::Parameters,
        state: Self::State,
    ) -> Result<Self::StateSummary, ContractError> {
        todo!()
    }

    fn get_state_delta(
        parameters: Self::Parameters,
        state: Self::State,
        summary: Self::StateSummary,
    ) -> Result<Self::StateDelta, ContractError> {
        todo!()
    }
}

#[test]
fn validate_test() -> Result<(), Box<dyn std::error::Error>> {
    todo!()
}