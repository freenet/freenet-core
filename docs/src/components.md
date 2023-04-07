# Components of Decentralized Software on Freenet

Delegates, contracts, and user-interface components each serve distinct roles in
the Freenet ecosystem. Contracts control public data, or "shared state".
Delegates act as the user's agent and can store private data on the user's
behalf, while apps serve as the user interface to contracts and delegates.

![Architectural Primitives Diagram](components.svg)

## Kernel

The kernel is the core of Freenet, it's the software that runs on the user's
computer. It's responsible for:

* Providing a user-friendly interface to access Freenet via a web browser
* Host the user's delegates and the private data they store
* Host contracts and their associated data on behalf of the network
* Manage communication between contracts, delegates, and UI componets

The kernel is written in Rust and is designed to be small (hopefully less than 5
MB), efficient, and to be able to run on a wide range of devices, from desktops
to mobile phones.

## User Interface

User Interface components are Freenet's version of a [single-page
application](https://en.wikipedia.org/wiki/Single-page_application). They are
built using web technologies such as HTML, CSS, and JavaScript, and are
distributed over Freenet and run in a web browser. UIs can create, retrieve, and
update contracts through a WebSocket connection to the local Freenet peer, as
well as communicate with delegates. 

Because UIs run in a web browser, they can be built using any web framework,
such as React, Angular, Vue.js, Bootstrap, and so on. 

### UI Use Cases

* A UI front-end for a messaging system similar to Gmail
* A UI front-end for a social network similar to Twitter
* A UI front-end for a delegate that manages secrets, similar to LastPass

## Contracts

Contracts in Freenet are [WebAssembly](https://webassembly.org) code that manage
and regulate public state. They can be likened to inodes in a filesystem, tables
in a database, or memory locations in a globally shared memory. Contracts define
the circumstances under which state can be modified and whether a given state is
allowed under the contract.

Contracts and their associated state reside on the Freenet network on peers
determined by the contract's location, which is derived from its WebAssembly
code and parameters. While a user's delegates are hosted on their local Freenet
peer, contracts are hosted on the network as a whole.

Contracts also outline how to merge two valid states, creating a new state that
incorporates both. This process ensures [eventual
consistency](https://en.wikipedia.org/wiki/Eventual_consistency) of the state in
Freenet, using an approach akin to
[CRDTs](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type).

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


## Delegates

Delegates are WebAssembly code components that serve as personal agents for
users, operating on their devices to manage private data and interact with
digital entities such as contracts, apps, and other delegates. These agents
provide a range of functionalities, including task execution, secret storage,
cryptographic operations, and communication with users to obtain permission for
specific actions. Delegates can be seen as an advanced and more powerful
alternative to cookies or local storage used in web browsers. With the user's
consent, delegates can be created by UI components or other delegates.

Unlike contracts which run on the network meaning that anything they do must be
verified by other peers, delegates run on the user's computer and can be trusted
to execute their code without verification.

Delegates must implement the
[ComponentInterface](https://github.com/freenet/locutus/blob/f1c8075e173f171c17ffa8d08803b2c9aea4ddf3/crates/locutus-stdlib/src/component_interface.rs#L121).

While contracts' state is public (but potentially encrypted), delegates state is
private. Example uses for delegates include:

* Storing and controlling the use of private data such as passwords, keys,
  tokens, and other sensitive information
* Participating on the user's behalf in decentralized systems like a web of
  trust
* Storing private data on behalf of the user, like contacts, or sent and
  received messages

### Origin Attestation

Delegates communicate with apps and other delegates using messages, a crucial
aspect of which is the delegate's ability to identify the origin of a message.
The origin is identified by the key associated with the app, contract, or other
delegate that sent the message. This key is generated cryptographically based on
the code and configuration parameters of the sender, enabling delegates to
verify the behavior of other delegates or apps with which they interact.

This allows for highly flexible composability, as components can trust the
behavior of the components they communicate with by verifying their code and
configuration parameters.

### Delegate Use Cases

* A key manager delegate is responsible for managing a user's private keys,
  other components can request that the key manager delegate sign messages
  or other data on their behalf.

* An inbox delegate is responsible for maintaining an inbox of messages sent to
  the user in a messaging system. It pulls messages from an inbox contract,
  decrypts them, and stores them locally where they can be queried by other
  components.
