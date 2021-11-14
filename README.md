### Purpose

A distributed, decentralized, key-value store in which keys are cryptographic contracts which determine what values are valid under that key.

The store is observable, allowing applications built on Locutus to listen for changes to values and be notified immediately. The cryptographic contracts are specified in web assembly.

This key-value store serves as a foundation for decentralized, scalable, and trustless alternatives to centralized services, including email, instant messaging, and social networks, many of which rely on closed proprietary protocols.

Locutus is implemented in Rust on top of the libp2p library and will be available across all major operating systems, Desktop and mobile.

### Value

The Internet has grown increasingly centralized over the past 25 years, such that a handful of companies now effectively control the Internet infrastructure. This happened because building decentralized applications is much more difficult than building their centralized equivalents.

Locutus' purpose is to make it easy to build decentralized services and also to provide reference implementations of the most popular services such as email and instant messaging.

We're building Locutus on the Rust implementation of libp2p meaning our developers will contribute improvements to this project, including code, testing, and more general feedback. benefitting systems built on libp2p, including FileCoin.

Our "chainless" smart contract approach will result in learnings for IPFS-native applications and FileCoin's smart contract initiative.

Lastly, our deployment of a libp2p application to the major mobile platforms will inform other IPFS on mobile efforts, particularly the low-bandwidth, low-power, and variable connectivity challenges of mobile.

### Relationship to "Fred"

"Fred" is the name of the software commonly known as Freenet, which was created by Ian Clarke and other volunteers in 2000 and continually maintained since then.

Locutus will be a general purpose, cryptographically secure, decentralized, distributed computation and communication platform. Anyone can write and run software on it, and that software can interact in ways that would be difficult otherwise.

Locutus is not intended to replace Fred, which is very mature and has an active group of volunteer developers who maintain it.

Key differences between Locutus and Fred:

* Locutus is designed to support realtime communication, fast enough for IM (less than 1 second). Fred has been improving in this regard but is still limited to several minutes. This is fine for many applications, but rules out IM, social networking, among others.

* Fred is largely a research platform, and this has limited its adoption. Locutus is being designed from the ground up to be no more difficult to use than a web browser.

* Fred has a strong focus on anonymity. Anonymity is not a core design goal for Locutus itself, but systems that provide greater privacy can be built on top of it.

* Fred is implemented in Java, Locutus is implemented in Rust, Java has been a signficant impediment to getting Fred on the desktop

### Status

Locutus is currently in development with an initial release planned for Q1 2022.
