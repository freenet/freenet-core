<div align="center">
  <!-- Github Actions -->
  <a href="https://github.com/freenet/freenet-core/actions/workflows/ci.yml">
    <img src="https://img.shields.io/github/actions/workflow/status/freenet/locutus/ci.yml?branch=main&label=tests&style=flat-square" alt="continuous integration status" />
  </a>
  <a href="https://crates.io/crates/locutus">
    <img src="https://img.shields.io/crates/v/locutus.svg?style=flat-square"
    alt="Crates.io version" />
  </a>
  <a href="https://matrix.to/#/#freenet-locutus:matrix.org">
    <img src="https://img.shields.io/matrix/freenet-locutus:matrix.org?label=matrix&logo=matrix&style=flat-square" alt="matrix" />
  </a>
  <a href="https://docs.rs/locutus">
    <img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square&label=api%20docs"
      alt="docs.rs docs" />
  </a>
</div>

# Freenet

The Internet has grown increasingly centralized over the past 25 years, such
that a handful of companies now effectively control the Internet infrastructure.
The public square is privately owned, threatening freedom of speech and
democracy.

Freenet is a software platform that makes it easy to create decentralized
alternatives to today's centralized tech companies. These decentralized apps
will be easy to use, scalable, and secured through cryptography.

To learn more about Freenet as a developer read [The User
Manual](https://docs.freenet.org/). For an introduction to Freenet watch **Ian's
talk and Q&A** - [YouTube](https://youtu.be/d31jmv5Tx5k) /
[Vimeo](https://vimeo.com/manage/videos/740461100).

## Status

Freenet is currently under development. Using our [development
guide](https://docs.freenet.org/tutorial.html), developers can experiment with
building decentralized applications using our SDK and testing them locally.

## Applications

Examples of what can be built on Freenet include:

* Decentralized email (with a gateway to legacy email via the @freenet.org
  domain)
* Decentralized microblogging (think Twitter or Facebook)
* Instant Messaging (Whatsapp, Signal)
* Online Store (Amazon)
* Discussion (Reddit, HN)
* Video discovery (Youtube, TikTok)
* Search (Google, Bing)

All will be completely decentralized, scalable, and cryptographically secure. We
want Freenet to be useful out-of-the-box, so we plan to provide reference
implementations for some or all of these.

## How does it work?

Freenet is a decentralized key-value database. It uses the same [small
world](https://freenetproject.org/assets/papers/lic.pdf) routing algorithm as
the original Freenet design, but each key is a cryptographic contract
implemented in [Web Assembly](https://webassembly.org/), and the value
associated with each contract is called its *state*. The role of the
cryptographic contract is to specify what state is allowed for this contract,
and how the state is modified.

A very simple contract might require that the state is a list of messages, each
signed with a specific cryptographic keypair. The state can be updated to add
new messages if appropriately signed. Something like this could serve as the
basis for a blog or Twitter feed.

Freenet is implemented in Rust and will be available across all major operating
systems, desktop and mobile.

## What is Locutus?

Locutus was the working title used for this successor to the original Freenet,
in March 2023 it was renamed to "Freenet" or "Freenet 2023", this repository was
renamed from `locutus` to `freenet-core` in September 2023.

## Stay up to date

[![Twitter
Follow](https://img.shields.io/twitter/follow/freenetorg?color=%2300EE00&logo=twitter&style=plastic)](https://twitter.com/FreenetOrg)

## Chat with us

We're in [#freenet-locutus](https://matrix.to/#/#freenet-locutus:matrix.org) on
[Matrix](https://matrix.org/). If you have questions you can also [ask
here](https://github.com/freenet/freenet-core/discussions).

Many developers are active in [r/freenet](https://www.reddit.com/r/Freenet/),
but remember that Reddit engages in political and ideological censorship so
don't make this your only point of contact with us.

## Acknowledgements and Funding

### Protocol Labs

In addition to creating the excellent
[libp2p](https://github.com/libp2p/rust-libp2p) which we use for low-level
transport, Protocol Labs has generously supported our work with a grant.

### FUTO

FUTO has generously awarded Freenet two Legendary Grants to support Freenet
development.

### Supporting Freenet

If you are in a position to fund our continued efforts please contact us on
[twitter](https://twitter.com/FreenetOrg) or by email at *ian at freenet dot
org*.

## License

This project is licensed under either of:

* Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or
  <http://opensource.org/licenses/MIT>)
