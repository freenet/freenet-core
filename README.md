### Locutus

*Note:* "Locutus" is the development codename for this project which is also known as "Freenet2".

[//]: # ([![Build Status](https://github.com/freenet/locutus/actions/workflows/build.yml/badge.svg)](https://github.com/freenet/locutus/actions/workflows/build.yml))

A distributed, decentralized, key-value store in which keys are cryptographic contracts which determine what values are valid under that key.

The store is observable, allowing applications built on Freenet2 to listen for changes to values and be notified immediately. The cryptographic contracts are specified in web assembly.

This key-value store serves as a foundation for decentralized, scalable, and trustless alternatives to centralized services, including email, instant messaging, and social networks, many of which rely on closed proprietary protocols.

Freenet2 is implemented in Rust on top of the libp2p library and will be available across all major operating systems, Desktop and mobile.

### Value

The Internet has grown increasingly centralized over the past 25 years, such that a handful of companies now effectively control the Internet infrastructure. This happened because building decentralized applications is much more difficult than building their centralized equivalents.

Freenet2's purpose is to make it easy to build decentralized services and also to provide reference implementations of the most popular services such as email and instant messaging.

We're building Freenet2 on the Rust implementation of libp2p meaning our developers will contribute improvements to this project, including code, testing, and more general feedback. benefitting systems built on libp2p, including FileCoin.

Our "chainless" smart contract approach will result in learnings for IPFS-native applications and FileCoin's smart contract initiative.

Lastly, our deployment of a libp2p application to the major mobile platforms will inform other IPFS on mobile efforts, particularly the low-bandwidth, low-power, and variable connectivity challenges of mobile.
