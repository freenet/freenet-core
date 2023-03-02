# Architecture

## Concepts

### Applications

This is the user interface that wishes to use the AFT system to send a message,
for example a decentralized instant messaging system. Applications will
typically run in a web browser and either implemented in JavaScript or
WebAssembly.

### Delegates

Delegates are webassembly code that act as intermediaries between the user and
various digital entities such as contracts, apps, and other delegates. They
perform tasks, store secrets, perform cryptographic operations, and communicate
with the user for permission to perform specific actions. They are like a much
more powerful version of cookies or local storage in a web browser. Delegates
can be created by applications or other delegates, with the user's permission.
Delegates implement the
[DelegateInterface](https://github.com/freenet/locutus/blob/f1c8075e173f171c17ffa8d08803b2c9aea4ddf3/crates/locutus-stdlib/src/component_interface.rs#L121).

### Contracts

Contracts are webassembly code implementing the
[ContractInterface](https://github.com/freenet/locutus/blob/f1c8075e173f171c17ffa8d08803b2c9aea4ddf3/modules/antiflood-tokens/contracts/token-allocation-record/src/lib.rs#L10).
