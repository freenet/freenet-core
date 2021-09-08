# Components

## Low-level transport

Establish direct connections between Freenet nodes, performing NAT hole-punching where
necessary. These connections will be encrypted to prevent snooping, and endpoints will be
verified to prevent main-in-the-middle attacks. 

Support the seconding of short messages over these connections, and also the streaming
of data to allow the transmission of larger data.

Provide a convenient interface to the low-level transport layer using struct serialization
and a callback mechanism for message responses to ensure the clarity and simplicity of
calling code.

Libraries:
* LibP2P

## Key-Value store

A persistent local store of keys and values in which keys are cryptographic contracts
specified in [WebAssembly](https://en.wikipedia.org/wiki/WebAssembly). These contracts
specify whether some data is valid, for example by checking a digital signature, a
generalization of the concept of [content addressable storage](https://en.wikipedia.org/wiki/Content-addressable_storage), 
as [pioneered](https://github.com/freenet/wiki/wiki/Signed-Subspace-Key) by FreenetV1.

The key/value pairs are stored locally in a lightweight database.

Libraries:
* [SQLite](https://sqlite.org/)
* [Wasmer](https://wasmer.io/)

## Small-world network

A small-world network, a type of [distributed hashtable](https://en.wikipedia.org/wiki/Distributed_hash_table),
is used to allow key/value pairs to be stored and retrieved globally on the network in a way that is
robust, decentralized, scalable, and secure.