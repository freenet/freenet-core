
<br>

<div align = center>

[![Badge Matrix]][Matrix]   
[![Badge Discord]][Discord]   
[![Badge Twitter]][Twitter]   

[![Badge Apache]][License Apache]    
![Badge Or]    
[![Badge MIT]][License MIT]

[![Badge CI]][CI]

<br>

# Locutus

*Software platform for creating decentralized alternatives* <br>
*to today's centralized tech companies with familiar tools* <br>
*like **[React]** and **[Vue.js]**.*

<br>
<br>

[![Button Documentation]][Documentation]   
[![Button Donate]][Donate]


<br>
<br>

*The project name - **Locutus** - may still be changed.*

*An **Early Prototype** is being worked* <br>
*on and expected for **August 2022**.*

</div>

<br>
<br>

## Mission

The Internet has grown increasingly centralized over <br>
the past 25 years, such that a handful of companies <br>
now effectively control the Internet's infrastructure.

The public square is privately owned, thus not only <br>
threatening freedom of speech but also democracy.

**Locutus** as an alternative platform wants to make <br>
decentralized apps be easy to use, scalable & secure <br>
through cryptography to regain these lost freedoms.

<br>
<br>

## News

### 2022   July 16th

*Ian gave a talk on Decentralized Reputation and Trust.*

[![Button YouTube]][16th YouTube]

### 2022   July 7th

*Ian gave an introductory talk on **Locutus**.*

[![Button YouTube]][7th YouTube]  
[![Button Vimeo]][7th Vimeo]

<br>
<br>

## Applications

*What can be built on Locutus:*

<br>

-   **Decentralized Micro-blogging**
    
    *Think Twitter or Facebook*

-   **Decentralized Email**

    *With a gateway to legacy email* <br>
    *via the @freenet.org domain*

-   **Instant Messaging**

    *Whatsapp , Signal*

-   **Video Discovery**

    *Youtube , TikTok*

-   **Online Stores**

    *Amazon*

-   **Discussions**
    
    *Reddit , HN*

-   **Search**
    
    *Google , Bing*

<br>

We want **Locutus** to be useful out-of-the-box, so we <br>
plan to provide reference implementations for some <br>
or all of these.

<br>
<br>

## Components

Decentralized services that can be used by other decentralized services:

### Reputation system

Allows users to build up reputation over time based on feedback from those they interact with. Think of the feedback system in services like Uber, but with Locutus it will be entirely decentralized and cryptographically secure. It can be used for things like spam prevention (with IM and email), or fraud prevention (with an online store).

This is conceptually similar to Freenet's [Web of Trust](http://www.draketo.de/english/freenet/friendly-communication-with-anonymity) plugin.

### Arbiters

Arbiters are trusted services that can perform tasks and authenticate the results, such as verifying that a contract had a particular state at a given time, or that external blockchains (Bitcoin, Ethereum, Solana etc) contain specific transactions. Trust is achieved through the reputation system.

## How does it work?

Locutus is a decentralized key-value database. It uses the same [small world](https://freenetproject.org/assets/papers/lic.pdf) routing algorithm as the original Freenet design, but each key is a cryptographic contract implemented in [Web Assembly](https://webassembly.org/), and the value associated with each contract is called its *state*. The role of the cryptographic contract is to specify what state is allowed for this contract, and how the state is modified.

A very simple contract might require that the state is a list of messages, each signed with a specific cryptographic keypair. The state can be updated to add new messages if appropriately signed. Something like this could serve as the basis for a blog or Twitter feed.

Locutus is implemented in Rust and will be available across all major operating systems, desktop and mobile.

<br>
<br>

## Acknowledgments

<br>

-   **[Protocol Labs]**

    *In addition to creating the excellent **[libp2p]** library,* <br>
    *which we use for low-level transport, they have* <br>
    *also generously supported our work with a grant.*

<br>


<!----------------------------------------------------------------------------->

[Documentation]: https://github.com/freenet/locutus/wiki/Glossary
[Protocol Labs]: https://protocol.ai/
[Twitter]: https://twitter.com/FreenetOrg
[Discord]: https://discord.gg/2kZuKNxYXv
[Vue.js]: https://vuejs.org/
[Matrix]: https://matrix.to/#/#freenet-locutus:matrix.org
[Donate]: https://freenetproject.org/pages/donate.html
[libp2p]: https://github.com/libp2p/rust-libp2p
[React]: https://reactjs.org/
[CI]: https://github.com/freenet/locutus/actions/workflows/ci.yml

[16th YouTube]: https://youtu.be/4L9pXIBAdG4
[7th YouTube]: https://www.youtube.com/watch?v=d31jmv5Tx5k
[7th Vimeo]: https://vimeo.com/740461100

[License Apache]: LICENSE-APACHE
[License MIT]: LICENSE-MIT


<!---------------------------------[ Badges ]---------------------------------->

[Badge Twitter]: https://img.shields.io/twitter/follow/freenetorg?color=1780bd&labelColor=1DA1F2&logoColor=white&logo=Twitter&style=for-the-badge&label=FreenetOrg
[Badge Discord]: https://img.shields.io/discord/917499817758978089?logoColor=white&style=for-the-badge&label=Discord&logo=Discord&labelColor=7289da&color=5d71b3
[Badge Matrix]: https://img.shields.io/matrix/freenet-locutus:matrix.org?logoColor=white&style=for-the-badge&label=matrix&logo=matrix&labelColor=0DBD8B&color=0b9f73
[Badge Apache]: https://img.shields.io/badge/License-Apache_2-961b1f?style=for-the-badge&labelColor=D22128
[Badge MIT]: https://img.shields.io/badge/License-MIT-ac8b11.svg?style=for-the-badge&labelColor=yellow
[Badge CI]: https://img.shields.io/github/workflow/status/freenet/locutus/CI?logoColor=white&style=for-the-badge&color=a81d59&labelColor=cb236c&logo=GitHub
[Badge Or]: https://img.shields.io/badge/OR-66b9c2?style=for-the-badge


<!---------------------------------[ Buttons ]--------------------------------->

[Button Documentation]: https://img.shields.io/badge/Documentation-1da1f2?style=for-the-badge&logoColor=white&logo=GitBook
[Button Donate]: https://img.shields.io/badge/Donate-cb236c?style=for-the-badge&logoColor=white&logo=GitHubSponsors
[Button YouTube]: https://img.shields.io/badge/YouTube-FF0000?style=flat&logoColor=white&logo=YouTube
[Button Vimeo]: https://img.shields.io/badge/Vimeo-1AB7EA?style=flat&logoColor=white&logo=Vimeo

