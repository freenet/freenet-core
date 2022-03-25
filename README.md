# Locutus 

[![Chat on Discord](https://img.shields.io/discord/917499817758978089?label=chat&logo=discord)](https://discord.gg/Q2FWzCqKQD)

### Purpose

The Internet has grown increasingly centralized over the past 25 years, such that a handful of companies now effectively control the Internet infrastructure. This happened because building decentralized applications is much more difficult than building their centralized equivalents.

Locutus is a distributed, decentralized application layer for the Internet. It provides a foundation for decentralized, scalable, and trustless alternatives to centralized services, including email, instant messaging, search engines, and social networks. It solves the hardest part of building decentralized applications.

### Architecture

A decentralized, scalable key-value store in which values are arbitrary data we call *state*, and keys are *cryptographic contracts* that control 
the creation and modification of its associated state. Contracts are implemented in [Web Assembly](https://webassembly.org/). Any participant in the network can request a contract's state, and also *subscribe* to state changes.

Locutus is implemented in Rust and will be available across all major operating systems, desktop and mobile.

### Status

We're working hard and expect an early prototype in May 2022. If you're a Rust developer and would like to help please talk to us in [#locutus](https://discord.gg/2kZuKNxYXv) on Discord. You can also support Freenet through a [donation](https://freenetproject.org/pages/donate.html).

### Relationship to "Fred"

"Fred" is the software commonly known as Freenet, which Ian Clarke and other volunteers created in 2000 and have continually maintained since then.

Locutus will be a general-purpose, cryptographically secure, decentralized, distributed computation and communication platform.

Locutus won't replace Fred; they're trying to solve different (but related) problems. Key differences between Locutus and Fred:

* Locutus is designed to support real-time communication, fast enough for IM - less than 1 second, compared to 1 minute for Fred. 

* Fred is primarily a research platform, and this has limited its adoption. While it continues to improve, ease of use has always been an issue for Fred. Locutus is being designed from the ground up to be no more difficult to use than a web browser.

* Fred has [signed subspace keys](https://freenetproject.org/pages/documentation.html), Locutus' contracts are a greatly generalized version of this concept
  that form the building blocks of a flexible distributed computation platform.

* Fred has a strong focus on anonymity. Anonymity is not a core design goal for Locutus itself, but systems that provide greater privacy can be built on top of it.

* Fred is implemented in Java, Locutus is implemented in Rust. Rust apps can be far more lightweight than their Java equivalents because they don't need the JVM. 

### Name

Locutus is the development name for this software; it will probably change before launch.

### Chat with us

We're in [#locutus](https://discord.gg/2kZuKNxYXv) on Discord.

## License

This project is licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
  http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  http://opensource.org/licenses/MIT)

at your option.
