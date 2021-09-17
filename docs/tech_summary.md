# F2 technical summary

*A decentralized application layer for the Internet*

## Motivation

In 2021, a handful of companies control almost the entire Internet infrastructure, with the ability and willingness to arbitrarily 
censor or exclude any person or organization without recourse or accountability.

F2 makes it easy to build entirely decentralized websites, social networking apps, online stores, search 
engines, email, instant messaging, and discussion forums - the entire range of Internet usecases, and will be distributed
with versions of each of these apps, likely starting with email.

## Platform

F2 is an entirely decentralized key-value store with [observer semantics](https://en.wikipedia.org/wiki/Observer_pattern), 
where keys are cryptographic contracts that specify what values are valid for key. This is a generalization of the
concept of "Signed Subspace Keys" from FreenetV1.

As with FreenetV1, decentralization and scalability is achived through a [small world network](https://en.wikipedia.org/wiki/Small-world_network).

This can be contrasted with the blockchain approach where every transaction must be broadcast globally, resulting in high [transaction fees](https://ycharts.com/indicators/ethereum_average_transaction_fee) that prohibit many applications (around US$5.50 per transaction in August 2021).

Unlike many popular cryptocurrencies, F2 also avoids any costly reliance on [proof of work](https://en.wikipedia.org/wiki/Proof_of_work), 
which was estimated to consume 148TWh per year in May 2021, or around 22 million metric tons of carbon dioxide emissions per year, higher than many countries.

In addition to storage and retrieval of values under keys, F2 supports [observer semantics](https://en.wikipedia.org/wiki/Observer_pattern)
so applications can listen for changes to key values and be notified in realtime when they occur.

For flexibility and efficiency, contracts are specified in [web assembly](https://en.wikipedia.org/wiki/WebAssembly), designed by the
World Wide Web Consortium as a replacement for in-browser JavaScript and rapidly growing in support and adoption. This will allow
new applications to be built on F2 without needing to constantly upgrade the node software.

F2 is implemented in the [Rust](https://www.rust-lang.org/) programming language, which was selected for its speed,
robust security, and efficiency.

## Applications

* Email (Gmail, ProtonMail)
* Microblogging (Twitter, Facebook)
* Instant Messaging (Whatsapp, Telegram)
* Search (Google, Bing, Duck Duck Go)
* Online Store (Amazon, Ebay, Etsy)

## Past Milestones

### July 2020

* Design and prototyping began

### August 2021

* Rust implementation begins

### September 2021

* Second developer joins the team

## Upcoming Milestones

### September 2021

* Low-level networking and infrastructure
* Ring topology

### October 2021

* Key-value store
* Web Assembly contracts
* Get/put/subscribe protocols

### November 2021

* Key-value store WebSocket API
* Email app
* freenet.org email gateway

### December 2021

* Microblogging app
* IM app

### January 2022

* Online store app

## History

The original Freenet (FreenetV1) was based on the author's 1999 [paper](http://citeseer.ist.psu.edu/viewdoc/summary?doi=10.1.1.32.3665) 
"A Distributed Decentralised Information Storage and Retrieval System". Freenet was the first distributed, decentralized, encrypted 
peer-to-peer network, and pioneered concepts like [small world networks](https://en.wikipedia.org/wiki/Small-world_network), 
and cryptographic contracts (also known as [signed subspace keys](https://freenetproject.org/papers/freenet-ieee.pdf)).
