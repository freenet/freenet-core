## Contracts

Contracts in Freenet are [WebAssembly](https://webassembly.org) components that
manage and regulate public state. They can be likened to inodes in a filesystem,
tables in a database, or memory locations in a globally shared memory. Contracts
define the circumstances under which state can be modified and whether a given
state is allowed.

Contracts and their associated state reside on the Freenet network on peers
determined by the contract's location, which is derived from its WebAssembly
code and parameters. While a user's delegates are hosted on their local Freenet
peer, contracts are hosted on the network as a whole.

Each contract is identified by a cryptographic hash, which is a combination of
its code and parameters, also referred to as its "key". This key is used to
identify the contract and to verify that the contract's code and parameters have
not been tampered with.

### State synchronization and merging

Contracts must provide a way to merge any two valid state, creating a new state
that incorporates both. This process ensures [eventual
consistency](https://en.wikipedia.org/wiki/Eventual_consistency) of the state in
Freenet, using an approach akin to
[CRDTs](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type). 

In mathematical jargon, the contract defines a [commutative
monoid](https://en.wikipedia.org/wiki/Monoid#Commutative_monoid) on the
contract's state.

As a simple example, if the contract's state is a single number, then the
contract could define the merging of two states as the sum of the two
numbers. Other merging functions include the product, maximum,
or minimum of the two numbers.

These operations individually are too simple to be useful, but they can be
combined with others to support the merging of complex states.

Naively, a contract would provide a single function that takes two states and
returns a merged state. However, this approach would be inefficient, as it
requires the entire state to be transferred between peers. Instead, contracts
provide three functions: `summarize_state`, `get_state_delta`, and
`update_state` which together allow states to be synchronized efficiently
between peers.

### Contract Use Cases

Take, for example, a public blog contract. The state of this contract would be
the blog's content, which consists of a list of blog posts. The code within the
contract dictates that new posts may only be added if they are signed by the
blog's owner, while the contract's parameters include the blog owner's public
key.

The contract's code would define the merging of two states as taking the union
of the two lists of posts, perhaps with maxium limit on the number of posts
retained in the state, the oldest posts being removed first.

The state summary might be a list of the hashes of the posts in the blog along
with the date of the oldest post. The state delta would be a list of new posts
to be added to the blog, and the update state function would add the new posts
to the blog's state, removing older posts if necessary.

### The ContractInterface trait

Rust contracts must implement the `ContractInterface` trait, which defines the
functions that the kernel calls to interact with the contract. This trait is
defined in the [locutus-stdlib](https://github.com/freenet/locutus/blob/main/crates/locutus-stdlib/src/contract_interface.rs#L424).

```rust,no_run,noplayground
{{#include ../../../crates/locutus-stdlib/src/contract_interface.rs:contractifce}}
```