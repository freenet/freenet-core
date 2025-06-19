<div align="center">
  <!-- Github Actions -->
  <a href="https://github.com/freenet/freenet-core/actions/workflows/ci.yml">
    <img src="https://img.shields.io/github/actions/workflow/status/freenet/freenet-core/ci.yml?branch=main&label=tests&style=flat-square" alt="continuous integration status" />
  </a>
  <a href="https://crates.io/crates/freenet">
    <img src="https://img.shields.io/crates/v/freenet.svg?style=flat-square"
    alt="Crates.io version" />
  </a>
  <a href="https://matrix.to/#/#freenet:matrix.org">
    <img src="https://img.shields.io/matrix/freenet:matrix.org?label=matrix&logo=matrix&style=flat-square" alt="matrix" />
  </a>
  <a href="https://docs.rs/freenet">
    <img src="https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square&label=api%20docs"
      alt="docs.rs docs" />
  </a>
</div>

This is the freenet-core repository. To learn more about Freenet please visit our website
at [freenet.org](https://freenet.org/).

# Decentralize Everything

Freenet is the internet as it should be—fully decentralized, designed to put you back in control. Imagine a global shared
computer where you can communicate and collaborate freely, without reliance on big tech. Freenet lets you regain your
digital independence.

Freenet is a peer-to-peer network that transforms users’ computers into a resilient, distributed platform on which anyone
can build decentralized services. Every peer contributes to a fault-tolerant collective, ensuring services are always
available and robust.

Today’s web is a series of siloed services, but every system built on Freenet is fully interoperable by default. Freenet
apps can be built with popular web frameworks, accessed through any browser just like the web.

## Build Instructions

Before installing anything you need to run the following in the repository,
or the commands will fail:

```bash
$ git submodule update --init --recursive
```

To install the Freenet core:

```bash
$ cargo install --path crates/core
```

Or for the fdev utility:

```bash
$ cargo install --path crates/fdev
```
