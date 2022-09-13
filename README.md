
<br>

<div align = center>

[![Badge Matrix]][Matrix]   
[![Badge Discord]][Discord]   
[![Badge Twitter]][Twitter]   

[![Badge Apache]][License Apache]
    **or**    
[![Badge MIT]][License MIT]

[![Badge CI]][CI]
<br>

# Locutus

</div>

<br>

The Internet has grown increasingly centralized over the past 25 years, such that a handful of companies now effectively control the Internet infrastructure. The public square is privately owned, threatening freedom of speech and democracy.

Locutus is a software platform that makes it easy to create decentralized alternatives to today's centralized tech companies. These decentralized apps will be easy to use, scalable, and secured through cryptography.

Build Locutus apps with familiar tools like [React](https://reactjs.org/) or [Vue.js](https://vuejs.org/).

### 1. News

* **16th July, 2022:** Ian gave a talk on Decentralized Reputation and Trust, [watch here](https://github.com/freenet/locutus/wiki/Decentralized-Reputation-and-Trust)
* **7th July, 2022:** Ian gave an introductory talk on Locutus which you can watch on [YouTube](https://www.youtube.com/watch?v=d31jmv5Tx5k) or [Vimeo](https://vimeo.com/740461100).

### 2. Applications

Examples of what can be built on Locutus include:

* Decentralized email (with a gateway to legacy email via the @freenet.org domain)
* Decentralized microblogging (think Twitter or Facebook)
* Instant Messaging (Whatsapp, Signal)
* Online Store (Amazon)
* Discussion (Reddit, HN)
* Video discovery (Youtube, TikTok)
* Search (Google, Bing)

All will be completely decentralized, scalable, and cryptographically secure. We want Locutus to be useful out-of-the-box, so we plan to provide reference implementations for some or all of these.

### 3. Components

Decentralized services that can be used by other decentralized services:

#### 3.1 Reputation system

Allows users to build up reputation over time based on feedback from those they interact with. Think of the feedback system in services like Uber, but with Locutus it will be entirely decentralized and cryptographically secure. It can be used for things like spam prevention (with IM and email), or fraud prevention (with an online store).

This is conceptually similar to Freenet's [Web of Trust](http://www.draketo.de/english/freenet/friendly-communication-with-anonymity) plugin.

#### 3.2 Arbiters

Arbiters are trusted services that can perform tasks and authenticate the results, such as verifying that a contract had a particular state at a given time, or that external blockchains (Bitcoin, Ethereum, Solana etc) contain specific transactions. Trust is achieved through the reputation system.

### 4. How does it work?

Locutus is a decentralized key-value database. It uses the same [small world](https://freenetproject.org/assets/papers/lic.pdf) routing algorithm as the original Freenet design, but each key is a cryptographic contract implemented in [Web Assembly](https://webassembly.org/), and the value associated with each contract is called its *state*. The role of the cryptographic contract is to specify what state is allowed for this contract, and how the state is modified.

A very simple contract might require that the state is a list of messages, each signed with a specific cryptographic keypair. The state can be updated to add new messages if appropriately signed. Something like this could serve as the basis for a blog or Twitter feed.

Locutus is implemented in Rust and will be available across all major operating systems, desktop and mobile.

### 5. Documentation

* [Glossary](https://github.com/freenet/locutus/wiki/Glossary)

### 6. Status

We're working hard and expect an early prototype in August 2022. 

You can support our work through a [donation](https://freenetproject.org/pages/donate.html).


### 8. Name

Locutus is the development name for this software; it will probably change before launch.

### 9. Chat with us

We're in [#locutus](https://discord.gg/2kZuKNxYXv) on Discord, and also [#freenet-locutus](https://matrix.to/#/#freenet-locutus:matrix.org) on [Matrix](https://matrix.org/). These rooms are bridged so it doesn't matter which you join. If you have questions you can also [ask here](https://github.com/freenet/locutus/discussions).

Many developers are active in [r/freenet](https://www.reddit.com/r/Freenet/), but remember that Reddit engages in political and ideological censorship so don't make this your only point of contact with us.

### 10. Acknowledgements and Funding

In addition to creating the excellent [libp2p](https://github.com/libp2p/rust-libp2p) which we use for low-level transport, Protocol Labs has 
generously supported our work with a grant.

If you are in a position to fund our continued efforts please contact us on [twitter](https://twitter.com/FreenetOrg) or by email at 
*ian at freenet dot org*.

  
<br>


<!----------------------------------------------------------------------------->

[Twitter]: https://twitter.com/FreenetOrg
[Discord]: https://discord.gg/2kZuKNxYXv
[Matrix]: https://matrix.to/#/#freenet-locutus:matrix.org
[CI]: https://github.com/freenet/locutus/actions/workflows/ci.yml

[License Apache]: LICENSE-APACHE
[License MIT]: LICENSE-MIT


<!---------------------------------[ Badges ]---------------------------------->

[Badge Twitter]: https://img.shields.io/twitter/follow/freenetorg?color=1780bd&labelColor=1DA1F2&logoColor=white&logo=Twitter&style=for-the-badge&label=FreenetOrg
[Badge Discord]: https://img.shields.io/discord/917499817758978089?logoColor=white&style=for-the-badge&label=Discord&logo=Discord&labelColor=7289da&color=5d71b3
[Badge Matrix]: https://img.shields.io/matrix/freenet-locutus:matrix.org?logoColor=white&style=for-the-badge&label=matrix&logo=matrix&labelColor=0DBD8B&color=0b9f73
[Badge Apache]: https://img.shields.io/badge/License-Apache_2-961b1f?style=for-the-badge&labelColor=D22128
[Badge MIT]: https://img.shields.io/badge/License-MIT-ac8b11.svg?style=for-the-badge&labelColor=yellow
[Badge CI]: https://img.shields.io/github/workflow/status/freenet/locutus/CI?logoColor=white&style=for-the-badge&color=a81d59&labelColor=cb236c&logo=GitHub
