
<br>

<div align = center>

[![Badge Matrix]][Matrix]   
[![Badge Discord]][Discord]   
[![Badge Twitter]][Twitter]

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

*The name - **Locutus** - may still be changed.*

*An **Early Prototype** is being worked* <br>
*on and expected for **August 2022**.*

<br>
<br>

## Mission

<br>

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

</div>

<br>

### 2022   July 16th

*Ian gave a talk on Decentralized Reputation and Trust.*

[![Button YouTube]][16th YouTube]

<br>

### 2022   July 7th

*Ian gave an introductory talk on **Locutus**.*

[![Button YouTube]][7th YouTube]  
[![Button Vimeo]][7th Vimeo]

<br>
<br>

<div align = center>

## Applications

*What can be built on **Locutus**.*

<br>

| Decentralized Micro-blogging | Decentralized Email | Search
|:----------------------------:|:-------------------:|:------:
| *Twitter , Facebook* | *With a gateway to legacy email* <br> *via the @freenet.org domain* | *Google , Bing*

<br>

| Instant Messaging | Video Discovery | Online Stores | Discussions
|:-----------------:|:---------------:|:-------------:|:-----------:
| *Whatsapp , Signal* | *Youtube , TikTok* | *Amazon* | *Reddit , HN* 

<br>

We want **Locutus** to be useful out-of-the-box, so we <br>
plan to provide reference implementations for some <br>
or all of these.

</div>

<br>
<br>

<!--  TODO : Move out of README  -->

## Components

*Decentralized services that can be used by one another.*

<br>

### Reputation System

Allows users to build up reputation over time <br>
based on feedback from those they interact with.

Think of the feedback system in services such as Uber, <br>
but with **Locutus** it will be completely decentralized & <br>
cryptographically secure.

<br>

It can be used for things like:

-   **Spam Prevention**

    *With IM and email*
    
    <br>
    
-   **Fraud prevention**
    
    *With an online store*

<br>

This is conceptually similar to <br>
**Freenet**'s **[Web of Trust]** plugin.

<br>

### Arbiters

Arbiters are trusted services that can perform tasks <br>
and authenticate the results, such as verifying that <br>
a contract had a particular state at a given time, or <br>
that external blockchains - like Bitcoin , Ethereum , <br>
Solana - contain specific transactions.

Trust is achieved through the reputation system.

<br>
<br>

## How does it work?

**Locutus** is a decentralized key - value database.

It uses the same **[Small World]** routing algorithm as the <br>
original **Freenet** design, but each key is a cryptographic <br>
contract implemented in **[Web Assembly]** and the value <br>
associated with each contract is called its  `state` .

### Cryptography Contract

The role of a contract is to specify what <br>
state is allowed and how it is modified.

### State

A very simple contract might require that the state is a list of <br>
messages, each signed with a specific cryptographic keypair.

The state can be updated to add new messages <br>
if appropriately signed, which could for example <br>
serve as the basis for a blog or Twitter feed.

### Implementation

**Locutus** is implemented in **Rust** and will be available <br>
across all major operating systems, desktop and mobile.

<br>
<br>

## Acknowledgments

<br>

-   **[Protocol Labs]**

    *In addition to creating the excellent **[libp2p]** library,* <br>
    *which we use for low-level transport, they have* <br>
    *also generously supported our work with a grant.*

<br>
<br>

## Licensing

*Either one of the following licenses is applicable.*

[![Badge Apache]][License Apache]    
[![Badge MIT]][License MIT]

<br>


<!----------------------------------------------------------------------------->

[Documentation]: https://github.com/freenet/locutus/wiki/Glossary
[Protocol Labs]: https://protocol.ai/
[Web of Trust]: http://www.draketo.de/english/freenet/friendly-communication-with-anonymity
[Web Assembly]: https://webassembly.org/
[Small World]: https://freenetproject.org/assets/papers/lic.pdf
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
[Badge Or]: https://img.shields.io/badge/OR-1da1f2?style=for-the-badge


<!---------------------------------[ Buttons ]--------------------------------->

[Button Documentation]: https://img.shields.io/badge/Documentation-1da1f2?style=for-the-badge&logoColor=white&logo=GitBook
[Button Donate]: https://img.shields.io/badge/Donate-cb236c?style=for-the-badge&logoColor=white&logo=GitHubSponsors
[Button YouTube]: https://img.shields.io/badge/YouTube-FF0000?style=flat&logoColor=white&logo=YouTube
[Button Vimeo]: https://img.shields.io/badge/Vimeo-1AB7EA?style=flat&logoColor=white&logo=Vimeo

