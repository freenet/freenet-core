# Locutus 

[![Chat on Discord](https://img.shields.io/discord/917499817758978089?label=chat&logo=discord)](https://discord.gg/Q2FWzCqKQD)

The Internet has grown increasingly centralized over the past 25 years, such that a handful of companies now effectively control the Internet infrastructure. The fact that the public square is now privately owned is a threat to freedom of speech, and therefore a threat to democracy.

Locutus is a software platform that makes it easy to create completely decentralized alternatives to today's centralized tech companies.

Decentralized apps on Locutus can be built with the tools developers already know, such as [React](https://reactjs.org/) or [Vue.js](https://vuejs.org/).

Locutus apps can often be bridged to their legacy centralized equivolents, to make adoption easier - an early example will be email.

### Architecture

A decentralized, scalable key-value store in which values are arbitrary data we call *state*, and keys are *cryptographic contracts* that control 
the creation and modification of its associated state. Contracts are implemented in [Web Assembly](https://webassembly.org/). Any participant in the network can request a contract's state, and also *subscribe* to state changes.

Locutus is implemented in Rust and will be available across all major operating systems, desktop and mobile.

### Status

We're working hard and expect an early prototype in May 2022. If you're a Rust developer and would like to help please talk to us in [#locutus](https://discord.gg/2kZuKNxYXv) on Discord. You can also support Freenet through a [donation](https://freenetproject.org/pages/donate.html).

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
