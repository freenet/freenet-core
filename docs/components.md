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

Relevant libraries:
* [SQLite](https://sqlite.org/)
* RocksDB
* LMDB
* [Sled](https://github.com/spacejam/sled)
* [Percy](https://persy.rs/)
* [Wasmer](https://wasmer.io/)

## Small-world network

A [small-world network](https://en.wikipedia.org/wiki/Small-world_routing),
 a type of [distributed hashtable](https://en.wikipedia.org/wiki/Distributed_hash_table),
is used to allow key/value pairs to be stored and retrieved globally on the network in a way that is
robust, decentralized, scalable, and secure. The practical use of small world networks for this
purpose was pioneered by FreenetV1.

A peer is assigned a location, a floating point value between 0.0 and 1.0 organized in a ring such that the
distance between 0.0 and 1.0 is zero. The network topology is arranged such that the probability of a connection
existing between two peers is inversely proportional to the distance between them.

As peers join and leave the network existing peers will accept or reject new connections in order to
maintain the optimal network topology.

## Load balancing

Peers keep track of the bandwidth and CPU resources they consume while fulfilling requests from other peers,
the Wasmer WebAssembly library supports resource metering to facilitate this. Peers consuming excessive
resources may be throttled or disconnected. Peers will generally try to be helpful provided it doesn't
damage their reputations with other peers.

## 