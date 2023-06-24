## Contracts

Contracts in Freenet are [WebAssembly](https://webassembly.org) components that
manage and regulate public state. They can be likened to inodes in a filesystem,
tables in a database, or memory locations in a globally shared memory. Contracts
define the circumstances under which state can be modified and whether a given
state is allowed.

Contracts and their associated state reside on the Freenet
[network](../architecture/p2p-network.md) on peers determined by the contract's
location, which is derived from its WebAssembly code and parameters. While a
user's delegates are hosted on their local Freenet peer, contracts are hosted on
the network as a whole.

Contracts also outline how to merge two valid states, creating a new state that
incorporates both. This process ensures [eventual
consistency](https://en.wikipedia.org/wiki/Eventual_consistency) of the state in
Freenet, using an approach akin to
[CRDTs](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type). The contract
defines a [commutative monoid](https://en.wikipedia.org/wiki/Monoid#Commutative_monoid)
on the contract's state.

Each contract is identified by a cryptographic hash, which is a combination of
its code and parameters, also referred to as its "key". This key is used to
identify the contract and to verify that the contract's code and parameters have
not been tampered with.

### Contract Use Cases

Take, for example, a public blog contract. The state of this contract would be
the blog's content, which consists of a list of blog posts. The code within the
contract dictates that new posts may only be added if they are signed by the
blog's owner, while the contract's parameters include the blog owner's public
key.