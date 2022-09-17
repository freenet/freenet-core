# Contract Interface

Locutus contracts must implement the contract interface[^ifacesrc]:

```rust
pub trait ContractInterface {
    /// Verify that the state is valid, given the parameters.
    fn validate_state(parameters: Parameters<'static>, state: State<'static>) -> bool;

    /// Verify that a delta is valid - at least as much as possible.
    fn validate_delta(parameters: Parameters<'static>, delta: StateDelta<'static>) -> bool;

    /// Update the state to account for the state_delta, assuming it is valid.
    fn update_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        delta: StateDelta<'static>,
    ) -> Result<UpdateModification, ContractError>;

    // FIXME: should return a result type, instead
    /// Generate a concise summary of a state that can be used to create deltas
    /// relative to this state.
    fn summarize_state(
        parameters: Parameters<'static>,
        state: State<'static>,
    ) -> StateSummary<'static>;

    // FIXME: should return a result type, instead
    /// Generate a state delta using a summary from the current state.
    /// This along with [`Self::summarize_state`] allows flexible and efficient
    /// state synchronization between peers.
    fn get_state_delta(
        parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> StateDelta<'static>;
}
```

[^ifacesrc]: This interface is subject to change, find the latest version in [interface.rs](https://github.com/freenet/locutus/blob/main/crates/locutus-stdlib/src/interface.rs#L76).