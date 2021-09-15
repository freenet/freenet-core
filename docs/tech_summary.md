# FreenetV2 technical summary

*A decentralized application layer for the Internet*

## Motivation

In 2021 a handful of companies control almost the entire Internet infrastructure, with the power to arbitrarily censor or exclude any person or organization without recourse.

F2 will enable the creation of entirely decentralized websites, social networking platforms, online stores, search 
engines, email, instant messaging, and discussion forums, while providing a framework that ensures interoperability between th?F2

## Design

FreenetV2 is an entirely decentralized key-value store with [observer semantics](https://en.wikipedia.org/wiki/Observer_pattern), 
where keys are cryptographic contracts that specify what values are valid for key. This is a generalization of the
concept of "Signed Subspace Keys" from FreenetV1.

As with FreenetV1, decentralization and scalability is achived through a [small world ring](https://en.wikipedia.org/wiki/Small-world_network).

This can be contrasted with the blockchain approach where every transaction must be broadcast globally, resulting in high [transaction fees](https://ycharts.com/indicators/ethereum_average_transaction_fee) that prohibit many applications (around US$5.50 per transaction in August 2021).

Unlike many popular cryptocurrencies, FreenetV2 also avoids any costly reliance on [proof of work](https://en.wikipedia.org/wiki/Proof_of_work), 
which was estimated to consume 148TWh per year in May 2021, or around 22 million metric tons of carbon dioxide emissions per year, higher than many countries.

In addition to storage and retrieval of values under keys, FreenetV2 supports [observer semantics](https://en.wikipedia.org/wiki/Observer_pattern)
so applications can listen for changes to key values and be notified in realtime when they occur.

For flexibility and efficiency, contracts are specified in [web assembly](https://en.wikipedia.org/wiki/WebAssembly), designed by the
World Wide Web Consortium as a replacement for in-browser JavaScript and rapidly growing in support and adoption. This will allow
new applications to be built on FreenetV2 without needing to constantly upgrade the node software.

FreenetV2 is implemented in the [Rust](https://www.rust-lang.org/) programming language.

## Services

### Microblogging

*Comparable to:* Twitter, Facebook

### Instant Messaging

*Comparable to:* Whatsapp, Telegram

### Search

*Comparable to:* Google, Bing, Duck Duck Go

### Store

*Comparable to:* Amazon, Ebay, Etsy

## History

The original Freenet (FreenetV1) was based on the author's 1999 [paper](http://citeseer.ist.psu.edu/viewdoc/summary?doi=10.1.1.32.3665) 
"A Distributed Decentralised Information Storage and Retrieval System". Freenet was the first distributed, decentralized, encrypted 
peer-to-peer network, and pioneered concepts like [small world networks](https://en.wikipedia.org/wiki/Small-world_network), 
and cryptographic contracts (also known as [signed subspace keys](https://freenetproject.org/papers/freenet-ieee.pdf)).
