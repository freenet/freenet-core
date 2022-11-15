<div align="center">
  <!-- Github Actions -->
  <a href="https://github.com/freenet/locutus/actions/workflows/ci.yml">
    <img src="https://img.shields.io/github/workflow/status/freenet/locutus/CI?label=CI&style=flat-square" alt="continuous integration status" />
  </a>
  <a href="https://crates.io/crates/locutus">
    <img src="https://img.shields.io/crates/v/locutus.svg?style=flat-square"
    alt="Crates.io version" />
  </a>
  <a href="https://discord.gg/2kZuKNxYXv">
    <img src="https://img.shields.io/discord/917499817758978089?style=flat-square&label=discord&logo=discord" alt="discord" />
  </a>
  <a href="https://matrix.to/#/#freenet-locutus:matrix.org">
    <img src="https://img.shields.io/matrix/freenet-locutus:matrix.org?label=matrix&logo=matrix&style=flat-square" alt="matrix" />
  </a>
  <a href="https://docs.rs/locutus">
    <img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square&label=api%20docs"
      alt="docs.rs docs" />
  </a>
</div>

# Locutus

The Internet has grown increasingly centralized over the past 25 years, such that a handful of companies now effectively control the Internet infrastructure. The public square is privately owned, threatening freedom of speech and democracy.

Locutus is a software platform that makes it easy to create decentralized alternatives to today's centralized tech companies. These decentralized apps will be easy to use, scalable, and secured through cryptography.

To learn more about Locutus as a developer read [The Locutus Book](https://docs.freenet.org/). For an introduction to Locutus watch **Ian's talk and Q&A** - [YouTube](https://youtu.be/d31jmv5Tx5k) / [Vimeo](https://vimeo.com/manage/videos/740461100).

## Status

Locutus is currently under development. Using our [development guide](https://docs.freenet.org/dev-guide.html), developers can experiment with building decentralized applications using our SDK and testing them locally. We expect the Locutus network to go live in January 2023.

## Applications

Examples of what can be built on Locutus include:

* Decentralized email (with a gateway to legacy email via the @freenet.org domain)
* Decentralized microblogging (think Twitter or Facebook)
* Instant Messaging (Whatsapp, Signal)
* Online Store (Amazon)
* Discussion (Reddit, HN)
* Video discovery (Youtube, TikTok)
* Search (Google, Bing)

All will be completely decentralized, scalable, and cryptographically secure. We want Locutus to be useful out-of-the-box, so we plan to provide reference implementations for some or all of these.

## How does it work?

Locutus is a decentralized key-value database. It uses the same [small world](https://freenetproject.org/assets/papers/lic.pdf) routing algorithm as the original Freenet design, but each key is a cryptographic contract implemented in [Web Assembly](https://webassembly.org/), and the value associated with each contract is called its *state*. The role of the cryptographic contract is to specify what state is allowed for this contract, and how the state is modified.

A very simple contract might require that the state is a list of messages, each signed with a specific cryptographic keypair. The state can be updated to add new messages if appropriately signed. Something like this could serve as the basis for a blog or Twitter feed.

Locutus is implemented in Rust and will be available across all major operating systems, desktop and mobile.

## Stay up to date

[![Twitter Follow](https://img.shields.io/twitter/follow/freenetorg?color=%2300EE00&logo=twitter&style=plastic)](https://twitter.com/FreenetOrg)

## Chat with us

We're in [#locutus](https://discord.gg/2kZuKNxYXv) on Discord, and also [#freenet-locutus](https://matrix.to/#/#freenet-locutus:matrix.org) on [Matrix](https://matrix.org/). These rooms are bridged so it doesn't matter which you join. If you have questions you can also [ask here](https://github.com/freenet/locutus/discussions).

Many developers are active in [r/freenet](https://www.reddit.com/r/Freenet/), but remember that Reddit engages in political and ideological censorship so don't make this your only point of contact with us.

## Acknowledgements and Funding

### Protocol Labs

In addition to creating the excellent [libp2p](https://github.com/libp2p/rust-libp2p) which we use for low-level transport, Protocol Labs has
generously supported our work with a grant.

### FUTO

FUTO has generously awarded Freenet a Legendary Grant to support Locutus development.

### Supporting Locutus

If you are in a position to fund our continued efforts please contact us on [twitter](https://twitter.com/FreenetOrg) or by email at
*ian at freenet dot org*.

## License

This project is licensed under either of:

* Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or
  <http://opensource.org/licenses/MIT>)
