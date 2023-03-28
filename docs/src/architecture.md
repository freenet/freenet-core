# Architecture

## Concepts

### Overview

Delegates, contracts, and applications each serve distinct roles in the software ecosystem. Contracts function as shared models, representing the agreed-upon state that can be modified according to user-defined rules. Delegates, on the other hand, operate on the user's device and manage private state, such as keys, secret tokens, and message inboxes/outboxes. They also facilitate the user's interaction with contracts and apps by controlling access to private data. Apps serve as the user interface for contracts and delegates, running in the browser and built using various web technologies like React.

### Applications

This is the user interface that wishes to use the AFT system to send a message,
for example a decentralized instant messaging system. Applications will
typically run in a web browser and either implemented in JavaScript or
WebAssembly.

### Contracts

Contracts in Freenet are WebAssembly code that manage and regulate public state. They can be likened to inodes in a filesystem, tables in a database, or memory locations in a globally shared memory. As the fundamental unit of shared state in Freenet, contracts define the circumstances under which state can be modified and whether a given state is allowed under the contract.

Each contract is identified by a cryptographic hash, which is a combination of its code and parameters, also referred to as its "key." This key corresponds to the value in a global key-value store.

Take, for example, a public blog contract. The state of this contract would be the blog's content, which consists of a list of blog posts. The code within the contract dictates that new posts may only be added if they are signed by the blog's owner, while the contract's parameters include the blog owner's public key.

Contracts also outline how to merge two valid states, creating a new state that incorporates both. This process ensures eventual consistency of the state in Freenet, using an approach akin to [CRDTs](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type).

### Delegates

Delegates are WebAssembly code components that serve as personal agents for users, operating on their devices to manage private data and interact with digital entities such as contracts, apps, and other delegates. These agents provide a range of functionalities, including task execution, secret storage, cryptographic operations, and communication with users to obtain permission for specific actions. Delegates can be seen as an advanced and more powerful alternative to cookies or local storage used in web browsers. With the user's consent, delegates can be created by applications or other delegates.

Delegates must implemenent the [ComponentInterface](https://github.com/freenet/locutus/blob/f1c8075e173f171c17ffa8d08803b2c9aea4ddf3/crates/locutus-stdlib/src/component_interface.rs#L121).

While contract states are public, delegates handle private state. However, it is important to note that public contract state can be encrypted if necessary, such as in the case of an inbox for a messaging system.

#### Origin Attestation

Delegates communicate with apps and other delegates using messages, a crucial aspect of which is the delegate's ability to identify the origin of a message. The origin is identified by the key associated with the app, contract, or other delegate that sent the message. This key is generated cryptographically based on the code and configuration parameters of the sender, enabling delegates to verify the behavior of other delegates or apps with which they interact.

This critical feature allows for highly flexible composability, as components can trust the behavior of the components they communicate with by verifying their code and configuration parameters.