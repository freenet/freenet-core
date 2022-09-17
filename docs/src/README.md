# Introduction

## What is Locutus?

The heart of Locutus is a global, decentralized key-value store. The values in the store are arbitrary blocks of data, called the contract's "state." The keys are cryptographic contracts that specify what state is permissible and how the state can be modified over time.

Locutus is a true peer-to-peer network, data is stored in a distributed fashion across all the peers participating in the network.

Applications on Locutus can be built in any language that is supported by web browsers, including JavaScript and WebAssembly. These applications are distributed over Locutus and can create, retrieve, and update contracts through a websocket connection to the local Locutus peer.

## Writing a Contract

Locutus contracts can be written in any language that compiles to webassembly. This includes [Rust](https://www.rust-lang.org/), [TypeScript](https://www.typescriptlang.org/), and [AssemblyScript](https://www.assemblyscript.org/), among many others.

A contract consists of the webassembly code itself and its "parameters," which are additional data like cryptographic keys. This makes it easy to configure contracts without having to recompile them.

A contract can be retrieved using a key, which is a cryptographic hash derived from the contract's webassembly code together with it's parameters.

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

Contracts are designed to be extremely flexible. they can be used to create decentralized data-structures like hashmaps, inverted indices for keyword search, or efficient buffers for streaming audio and video.

A contract must implement functions that do the following:

- Verify that the contract state is valid for this contract and parameters
- Verify that an update to a contract, a "delta", is valid

## Distributing your App on Locut

_TODO_
