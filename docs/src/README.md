**NOTE:** This document is a work in progress. You can [submit an issue](https://github.com/freenet/freenet-core/issues/new?labels=A-documentation) if you find a problem or have a suggestion. The source for this documentation is in our repository at [freenet-core/docs/src](https://github.com/freenet/freenet-core/tree/main/docs/src). We welcome pull requests.

# Introduction

## What is Freenet?

Freenet is a global, [observable](https://en.wikipedia.org/wiki/Small-world_network), decentralized key-value store. Values are arbitrary blocks of data, called the contract's "state." Keys are cryptographic contracts that specify:

- Whether a given state is permitted under this contract
- How the state can be modified over time
- How two valid states can be merged
- How to efficiently synchronize a contract's state between peers

Freenet is a true decentralized peer-to-peer network, and is robust and scalable, through its use of a [small-world network](https://en.wikipedia.org/wiki/Small-world_network).

Applications on Freenet can be built in any language that is supported by web browsers, including JavaScript and WebAssembly. These applications are distributed over Freenet and can create, retrieve, and update contracts through a WebSocket connection to the local Freenet peer.

## Writing a Contract

Freenet contracts can be written in any language that compiles to WebAssembly.
This includes [Rust](https://www.rust-lang.org/), and
[AssemblyScript](https://www.assemblyscript.org/), among many others.

A contract consists of the WebAssembly code itself and its "parameters," which are additional data like cryptographic keys. This makes it easy to configure contracts without having to recompile them.

A contract can be retrieved using a key, which is a cryptographic hash derived from the contract's WebAssembly code together with its parameters.

## Small world routing

Freenet peers self-organize into a [small-world network](https://en.wikipedia.org/wiki/Small-world_routing) to allow contracts to be found in a fast, scalable, and decentralized way.

Every peer in Freenet is assigned a number between 0 and 1 when it first joins the network, this is the peer's "location". The small world network topology ensures that peers with similar locations are more likely to be connected.

Contracts also have a location, which is derived from the contract's key. Peers cache contracts close to their locations.

## Writing an Application

Creating a decentralized application on Freenet is very similar to creating a normal web application. You can use familiar frameworks like React, Bootstrap, Angular, Vue.js, and so on.

The main difference is that instead of connecting to a REST API running on a server, the web application connects to the Freenet peer running on the local computer through a [WebSocket](https://en.wikipedia.org/wiki/WebSocket) connection.

Through this the application can:

- Create new contracts and their associated state
- Retrieve contracts and their state
- Modify contract state when permitted by the contract

## How to use Contracts

Contracts are extremely flexible. they can be used to create decentralized data structures like hashmaps, inverted indices for keyword search, or efficient buffers for streaming audio and video.

## Delegate Ecosystem

Applications in Freenet don't need to be built from scratch, they can be built on top of components provided by us or others.

### Reputation system

Allows users to build up reputation over time based on feedback from those they interact with. Think of the feedback system in services like Uber, but with Freenet it will be entirely decentralized and cryptographically secure. It can be used for things like spam prevention (with IM and email), or fraud prevention (with an online store).

This is conceptually similar to Freenet's [Web of Trust](http://www.draketo.de/english/freenet/friendly-communication-with-anonymity) plugin.

### Arbiters

Arbiters are trusted services that can perform tasks and authenticate the results, such as verifying that a contract had a particular state at a given time, or that external blockchains (Bitcoin, Ethereum, Solana etc) contain specific transactions. Trust is achieved through the reputation system.
