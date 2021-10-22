//! Inspired by rust-fsm. Brought in tree for modifying and tailoring it to
//! this application needs.

use super::OpError;

pub trait StateMachineImpl {
    /// The input alphabet.
    type Input;
    /// The set of possible states.
    type State;
    /// The output alphabet.
    type Output;

    /// The transition fuction that outputs a new state based on the current
    /// state and the provided input. Outputs `None` when there is no transition
    /// for a given combination of the input and the state.
    fn state_transition_from_input(
        _state: Self::State,
        _input: Self::Input,
    ) -> Option<Self::State> {
        None
    }

    fn state_transition(_state: &Self::State, _input: &Self::Input) -> Option<Self::State> {
        None
    }

    /// The output function that outputs some value from the output alphabet
    /// based on the current state and the given input. Outputs `None` when
    /// there is no output for a given combination of the input and the state.
    fn output_from_input(_state: Self::State, _input: Self::Input) -> Option<Self::Output> {
        None
    }

    fn output_from_input_as_ref(
        _state: &Self::State,
        _input: &Self::Input,
    ) -> Option<Self::Output> {
        None
    }
}

/// A convenience wrapper around the `StateMachine` trait that encapsulates the
/// state and transition and output function calls.
pub(crate) struct StateMachine<T: StateMachineImpl> {
    state: Option<T::State>,
}

impl<T> StateMachine<T>
where
    T: StateMachineImpl,
{
    /// Create a new instance of this wrapper which encapsulates the given
    /// state.
    pub fn from_state(state: T::State) -> Self {
        Self { state: Some(state) }
    }

    /// Consumes the provided input, gives an output and performs a state
    /// transition. If a state transition with the current state and the
    /// provided input is not allowed, returns an error.
    pub fn consume_to_state<CErr: std::error::Error>(
        &mut self,
        input: T::Input,
    ) -> Result<Option<T::Output>, OpError<CErr>> {
        let popped_state = self.state.take().expect("infallible");
        let output = T::output_from_input_as_ref(&popped_state, &input);
        if let Some(new_state) = T::state_transition_from_input(popped_state, input) {
            self.state = Some(new_state);
            Ok(output)
        } else {
            Err(OpError::IllegalStateTransition)
        }
    }

    pub fn consume_to_output<CErr: std::error::Error>(
        &mut self,
        input: T::Input,
    ) -> Result<Option<T::Output>, OpError<CErr>> {
        let popped_state = self.state.take().expect("infallible");
        if let Some(new_state) = T::state_transition(&popped_state, &input) {
            let output = T::output_from_input(popped_state, input);
            self.state = Some(new_state);
            Ok(output)
        } else {
            Err(OpError::IllegalStateTransition)
        }
    }

    /// Returns the current state.
    pub fn state(&mut self) -> &mut T::State {
        self.state.as_mut().expect("infallible")
    }
}
