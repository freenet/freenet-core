# Locutus 

[![Build Status](https://github.com/freenet/locutus/actions/workflows/ci.yml/badge.svg)](https://github.com/freenet/locutus/actions/workflows/ci.yml)
[![Chat on Discord](https://img.shields.io/discord/917499817758978089?label=chat&logo=discord)](https://discord.gg/Q2FWzCqKQD)

### Purpose

A distributed, decentralized, key-value store in which keys are cryptographic contracts that determine what values are valid under that key.

The store is observable, allowing applications built on Locutus to listen for changes to values and be notified immediately. The cryptographic contracts are specified in webassembly.

This key-value store serves as a foundation for decentralized, scalable, and trustless alternatives to centralized services, including email, instant messaging, and social networks, many of which rely on closed proprietary protocols.

Locutus is implemented in Rust on top of the libp2p library and will be available across all major operating systems, desktop and mobile.

### Value

The Internet has grown increasingly centralized over the past 25 years, such that a handful of companies now effectively control the Internet infrastructure. This happened because building decentralized applications is much more difficult than building their centralized equivalents.

Locutus' purpose is to make it easy to build decentralized services and also to provide reference implementations of the most popular services such as email and instant messaging.


### Relationship to "Fred"

"Fred" is the software commonly known as Freenet, which Ian Clarke and other volunteers created in 2000 and have continually maintained since then.

Locutus will be a general-purpose, cryptographically secure, decentralized, distributed computation and communication platform.

Locutus won't replace Fred; they're trying to solve different (but related) problems. Key differences between Locutus and Fred:

* Locutus is designed to support real-time communication, fast enough for IM - less than 1 second, compared to 1 minute for Fred. 

* Fred is primarily a research platform, and this has limited its adoption. While it continues to improve, ease of use has always been an issue for Fred. Locutus is being designed from the ground up to be no more difficult to use than a web browser.

* Fred has [signed subspace keys](https://freenetproject.org/pages/documentation.html), Locutus will have a greatly generalized version of this called cryptographic contracts which are specified in webassembly. These contracts are the software for the distributed, decentralized computer.

* Fred has a strong focus on anonymity. Anonymity is not a core design goal for Locutus itself, but systems that provide greater privacy can be built on top of it.

* Fred is implemented in Java, Locutus is implemented in Rust. Rust apps can be far more lightweight than their Java equivalents because they don't need the JVM. 

### Name

Locutus is the development name for this software; it will probably change before launch.

### Status

Locutus is currently in development, with an initial release planned for Q1 2022.

### Chat with us

We're in [#locutus](https://discord.gg/2kZuKNxYXv) on Discord.

## License

This project is licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
  http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  http://opensource.org/licenses/MIT)

at your option.
