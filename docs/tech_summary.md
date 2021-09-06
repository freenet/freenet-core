# FreenetV2 technical summary

*A decentralized application layer for the Internet*

## History

The original Freenet (FreenetV1) was based on the author's 1999 [paper](http://citeseer.ist.psu.edu/viewdoc/summary?doi=10.1.1.32.3665) 
"A Distributed Decentralised Information Storage and Retrieval System". Freenet was the first distributed, decentralized, encrypted 
peer-to-peer network, and pioneered concepts like [small world networks](https://en.wikipedia.org/wiki/Small-world_network), 
and cryptographic contracts (also known as [signed subspace keys](https://freenetproject.org/papers/freenet-ieee.pdf)).

## Goal

FreenetV2 is a reimagining of Freenet for 2021. 

It provides an alternative to the current highly centralized Internet architecture in which a handful of companies control almost the entire 
Internet infrastructure. It will enable the creation of entirely decentralized websites, social networking platforms, online stores, search 
engines, instant messaging, and discussion forums, while providing a framework that ensures interoperability between them.

## Design

FreenetV2 is an entirely decentralized key-value store with [observer semantics](https://en.wikipedia.org/wiki/Observer_pattern), 
where keys are cryptographic contracts that specify what values are valid for key. This is a generalization of the
concept of "Signed Subspace Keys" from FreenetV1.

As with FreenetV1, Decentralization and scalability is achived through a [small world ring](https://en.wikipedia.org/wiki/Small-world_network)-based
distributed hashtable.

In addition to storage and retrieval of values under keys, FreenetV2 supports [observer semantics](https://en.wikipedia.org/wiki/Observer_pattern)
so applications can listen for changes to key values and be notified in realtime when they occur.

For flexibility and efficiency, contracts are specified in [web assembly](https://en.wikipedia.org/wiki/WebAssembly), designed by the
World Wide Web Consortium as a replacement for in-browser JavaScript and rapidly growing in support and adoption.

FreenetV2 is implemented in the [Rust](https://www.rust-lang.org/) programming language.

## Karma

## Services

Contracts can reach out to, for example, check a HTTP API, and potentially use the result to store a value.

Untrustworthy behavior like lying about an API result can cost Karma.

Pre-committing random number generator

Pool membership to win karma