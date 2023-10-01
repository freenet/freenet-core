# Glossary

## Application

Software that uses Freenet as a back-end. This includes native software
distributed independenly of Freenet but which uses Freenet as a back-end
(perhaps bundling Freenet), and [web applications](glossary#web-application)
that are distributed over Freenet and run in a web browser.

## Contract

A contract is WebAssembly code with associated data like the contract state. The
role of the contract is to determine:

- Is the state valid for this contract?
- Under what circumstances can the state be modified or updated? (see Delta)
- How can two valid states be merged to produce a third valid state?

## Container Contract

A contract that contains an application or component as state, accessed through
the web proxy.

For example, if the contract id is
`6C2KyVMtqw8D5wWa8Y7e14VmDNXXXv9CQ3m44PC9YbD2` then visiting
`http://localhost:PORT/contract/web/6C2KyVMtqw8D5wWa8Y7e14VmDNXXXv9CQ3m44PC9YbD2`
will cause the application/component to be retrieved from Freenet, decompressed,
and sent to the browser where it can execute.

## Contract State

Data associated with a contract that can be retrieved by Applications and
Components. For efficiency and flexibility, contract state is represented as a
simple `[u8]` byte array.

## Delegate

A delegate is a piece of software that runs on the user's computer and acts on
the user's behalf. Similar to local storage in a web browser, delegates can
store private data on the user's computer and control how it is used. Delegates
can also interact with contracts, applications, and other delegates.

## Delta

Represents a modification to some state - similar to a
[diff](https://en.wikipedia.org/wiki/Diff) in source code. The exact format of a
delta is determined by the contract. A contract will determine whether a delta
is valid - perhaps by verifying it is signed by someone authorized to modify the
contract state. A delta may be created in response to a [State
Summary](glossary.md#state-summary) as part of the [State
Synchronization](glossary.md#state-synchronization) mechanism.

## Parameters

Data that forms part of a contract along with the WebAssembly code. This is
supplied to the contract as a parameter to the contract's functions. Parameters
are typically be used to configure a contract, much like the parameters of a
constructor function.

For example, the parameters could contain a hash of the state itself. The
contract would then use it to verify that the state hashes to that value. This
would create a contract that is guaranteed to contain the same state. In the
original Freenet, this was known as a [content hash
key](<http://justsolve.archiveteam.org/wiki/Content_Hash_Key_(Freenet)>).

## State Summary

Given a contract state, this is a small piece of data that can be used to
determine a [delta](glossary.md#delta) between two contracts as part of the
[state synchronization](glossary.md#state-synchronization) mechanism. The format
of a state summary is determined by the state's contract.

## State Synchronization

Given two valid states for a contract, the state synchronization mechanism
allows the states to be efficiently merged over the network to ensure [eventual
consistency](https://en.wikipedia.org/wiki/Eventual_consistency).

## Web Application

Software built on Freenet and distributed through Freenet.

Applications run in the browser and can be built with tools like React,
TypeScript, and Vue.js. An application may use multiple components and
[contracts](glossary.md#contract).

Applications are compressed and distributed via a [container
contract](glossary.md#container-contract).
