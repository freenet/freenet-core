# Glossary

## Application

Software that uses Locutus as a back-end. This includes native software distributed independenly of Locutus but which uses Locutus as a back-end (perhaps bundling Locutus), and [web applications](glossary#web-application) that are distributed over Locutus and run in a web browser.

## Contract

A contract is WebAssembly code with associated data like the contract state. The role of the contract is to determine:

- Is the state valid for this contract?
- Under what circumstances can the state be modified or updated? (see Delta)
- How can two valid states be merged to produce a third valid state?

## Container Contract

A contract that contains an application or component as state, accessed through the web proxy.

For example, if the contract id is `6C2KyVMtqw8D5wWa8Y7e14VmDNXXXv9CQ3m44PC9YbD2` then visiting `http://localhost:PORT/contract/web/6C2KyVMtqw8D5wWa8Y7e14VmDNXXXv9CQ3m44PC9YbD2` will cause the application/component to be retrieved from Locutus, decompressed, and sent to the browser where it can execute.

## Contract State

A **contract's state** is data associated with a contract that can be retrieved by Applications and Components. The role of a contract is to control how its state can be modified and efficiently propagated through the network.

## Delta

Represents a modification to some state - similar to a [diff](https://en.wikipedia.org/wiki/Diff) in source code. The exact format of a delta is determined by the contract. A contract will determine whether a delta is valid - perhaps by verifying it is signed by someone authorized to modify the contract state. A delta may be created in response to a [State Summary](glossary.md#state-summary) as part of the [State Synchronization](glossary.md#state-synchronization) mechanism.

## State Summary

Given a contract state, this is a small piece of data that can be used to determine a [delta](glossary.md#delta) between two contracts as part of the [state synchronization](glossary.md#state-synchronization) mechanism. The format of a state summary is determined by the state's contract.

## State Synchronization

Given two valid states for a contract, the state synchronization mechanism allows the states to be efficiently merged over the network to ensure [eventual consistency](https://en.wikipedia.org/wiki/Eventual_consistency).

## Web Application

Software built on Locutus and distributed through Locutus.

Applications run in the browser and can be built with tools like React, TypeScript, and Vue.js. An application may use multiple components and [contracts](glossary.md#contract).

Applications are compressed and distributed via a [container contract](glossary.md#container-contract).
