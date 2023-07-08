## Contracts

Freenet is a global decentralized key-value store, where the keys are
[WebAssembly](https://webassembly.org) code called contracts.

A contract has associated data called its state, which is publicly readable and
can be modified if permitted by the contract. Anyone can read the state of a
contract, or subscribe to be notified of changes to the state.

Contracts are identified by a hash of their code, which means that their identity
is tied to their functionality.

<!-- toc -->

## Contract Operation

### State synchronization and merging

#### Fundamental Concepts

Contracts need to provide a mechanism to merge any two valid states, creating a
new state that integrates both. This process ensures the eventual consistency of
contract states in Freenet, a concept similar to [Conflict-free Replicated Data
Types](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type).

In the language of mathematics, the contract defines a commutative monoid on
the contract's state. For example, if the contract's state is a single number,
then the contract could define the merging of two states as the sum of the two
numbers. However, these basic operations are too simple on their own but can be
combined with others to support the merging of more complex states.

#### Efficient State Synchronization

Efficiency in state synchronization is key. A naïve approach would be to provide
a single function that takes two states and returns a merged state. However,
this would be inefficient, as it necessitates the entire state to be transferred
between peers. Instead, Freenet contracts employ three functions:
`summarize_state`, `get_state_delta`, and `update_state`. Together, these functions
allow states to be synchronized efficiently between peers.

### Contract Use Cases

Consider a public blog contract. The state of this contract would be the blog's
content, including a list of blog posts. The contract's code stipulates that new
posts can only be added if they are signed by the blog's owner, with the owner's
public key included in the contract's parameters.

In this scenario, the contract's code would define the merging of two states as
taking the union of the two lists of posts. A maximum limit could be imposed on
the number of posts retained in the state, with the oldest posts being removed
first.

### Writing a Contract in Rust

#### The `ContractInterface` Trait

Rust contracts must implement the `ContractInterface` trait, which defines the
functions that the kernel calls to interact with the contract. This trait is
defined in the [locutus-stdlib](https://github.com/freenet/locutus/blob/main/crates/locutus-stdlib/src/contract_interface.rs#L424).

```rust,no_run,noplayground
{{#include ../../../crates/locutus-stdlib/src/contract_interface.rs:contractifce}}
```
