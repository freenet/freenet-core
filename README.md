# Locutus 

[![Chat on Discord](https://img.shields.io/discord/917499817758978089?label=chat&logo=discord)](https://discord.gg/Q2FWzCqKQD)

The Internet has grown increasingly centralized over the past 25 years, such that a handful of companies now effectively control the Internet infrastructure. The public square is privately owned, threatening freedom of speech and democracy. It is imperative that we provide a viable alternative to big tech, but which won't just become what it seeks to replace.

Locutus is a software platform that makes it easy to create decentralized alternatives to today's centralized tech companies. These decentralized apps will be easy to use, scalable, and secured through cryptography.

Build Locutus apps with familiar tools like [React](https://reactjs.org/) or [Vue.js](https://vuejs.org/), and can be "bridged" to the legacy centralized apps they replace.

### Applications

Examples of what can be built on Locutus include:

* Decentralized email (with a gateway to legacy email via the @freenet.org domain)
* Decentralized microblogging (think Twitter or Facebook)
* Instant Messaging (Whatsapp, Signal)
* Online Store (Amazon)
* Discussion (Reddit, HN)
* Video discovery (Youtube, TikTok)
* Search (Google, Bing)

All will be completely decentralized, scalable, and cryptographically secure. We want Locutus to be useful out-of-the-box, so we plan to provide reference implementations for some or all of these.

### Components

Decentralized services that can be used by other decentralized services:

#### Reputation system

Allows users to build up reputation over time based on feedback from those they interact with. This is similar to the feedback system in services like Uber, but with Locutus it will be entirely decentralized and cryptographically secure. This is similar to Freenet's [Web of Trust](https://github.com/freenet/plugin-WebOfTrust#:~:text=The%20Freenet%20plugin%20Web%20of,by%20drowning%20it%20in%20spam.) plugin. It can be used for things like spam prevention (with IM and email), or fraud prevention (with an online store).

#### Arbiters

Arbiters are trusted services that can perform tasks and authenticate the results, such as verifying that a contract had a particular state at a given time, or that external blockchains (Bitcoin, Ethereum, Solana etc) contain specific transactions. Trust is achieved through the reputation system.

#### Cryptocurrency

In addition to integrating with legacy blockchains via arbiters, we plan to build a native Locutus cryptocurrency. It will avoid high transaction costs because transactions won't need to be broadcast through the network thanks to Locutus' small-world network. Arbiters will be used to prevent [double spending](https://en.wikipedia.org/wiki/Double-spending).

### How does it work?

Locutus is a decentralized key-value database. It uses the same [small world](https://freenetproject.org/assets/papers/lic.pdf) routing algorithm as the original Freenet design, but each key is a cryptographic contract implemented in [Web Assembly](https://webassembly.org/), and the value associated with each contract is called its *state*. The role of the cryptographic contract is to specify what state is allowed for this contract, and how the state is modified.

A very simple contract might require that the state is a list of messages, each signed with a specific cryptographic keypair. The state can be updated to add new messages if appropriately signed. Something like this could serve as the basis for a blog or Twitter feed.

Locutus is implemented in Rust and will be available across all major operating systems, desktop and mobile.

### Status

We're working hard and expect an early prototype in May 2022. If you're a Rust developer and would like to help please talk to us in [#locutus](https://discord.gg/2kZuKNxYXv) on Discord. You can also support Freenet through a [donation](https://freenetproject.org/pages/donate.html).

### Name

Locutus is the development name for this software; it will probably change before launch.

### Chat with us

We're in [#locutus](https://discord.gg/2kZuKNxYXv) on Discord.

### Stay up to date

Follow [FreenetOrg](https://twitter.com/freenetorg) on Twitter.

## License

This project is licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
  http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  http://opensource.org/licenses/MIT)

at your option.
