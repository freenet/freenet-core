# Introduction

## What is Locutus?

Locutus is a global, [observable](https://en.wikipedia.org/wiki/Small-world_network), decentralized key-value store. Values are arbitrary blocks of data, called the contract's "state." Keys are cryptographic contracts that specify:

* Whether a given state permitted under this contract
* How the state can be modified over time
* How two valid states can be merged
* How to efficiently synchronize a contract's state between peers

Locutus is a true decentralized peer-to-peer network, and is robust and scalable, through its use of a [small-world network](https://en.wikipedia.org/wiki/Small-world_network).

Applications on Locutus can be built in any language that is supported by web browsers, including JavaScript and WebAssembly. These applications are distributed over Locutus and can create, retrieve, and update contracts through a WebSocket connection to the local Locutus peer.

## Writing a Contract

Locutus contracts can be written in any language that compiles to WebAssembly. This includes [Rust](https://www.rust-lang.org/), [TypeScript](https://www.typescriptlang.org/), and [AssemblyScript](https://www.assemblyscript.org/), among many others.

A contract consists of the WebAssembly code itself and its "parameters," which are additional data like cryptographic keys. This makes it easy to configure contracts without having to recompile them.

A contract can be retrieved using a key, which is a cryptographic hash derived from the contract's WebAssembly code together with its parameters.

## Small world routing

Locutus peers self-organize into a [small-world network](https://en.wikipedia.org/wiki/Small-world_routing) to allow contracts to be found in a fast, scalable, and decentralized way.

Every peer in Locutus is assigned a number between 0 and 1 when it first joins the network, this is the peer's "location". The small world network topology ensures that peers with similar locations are more likely to be connected.

Contracts also have a location, which is derived from the contract's key. Peers cache contracts close to their locations.

## Writing an Application

Creating a decentralized application on Locutus is very similar to creating a normal web application. You can use familiar frameworks like React, Bootstrap, Angular, Vue.js, and so on.

The main difference is that instead of connecting to a REST API running on a server, the app connects to the Locutus peer running on the local computer through a [WebSocket](https://en.wikipedia.org/wiki/WebSocket) connection.

Through this the application can:

- Create new contracts and their associated state
- Retrieve contracts and their state
- Modify contract state when permitted by the contract

## How to use Contracts

Contracts are designed to be extremely flexible. they can be used to create decentralized data structures like hashmaps, inverted indices for keyword search, or efficient buffers for streaming audio and video.

## Distributing your App on Locutus

