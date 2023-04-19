# Peer-to-Peer Network

## Overview

Freenet is a distributed, decentralized, and fault-tolerant key-value store
built on a [small-world
network](https://en.wikipedia.org/wiki/Small-world_network). It is resistent to
denial-of-service attacks and allows it to scale automatically to meet demand.
The store is observable, which means that users can subscribe to updates for
specific keys and receive notifications as soon as changes occur.

![Small World Network](p2p-network.svg)

## Peers in Freenet

A peer in Freenet is a computer running the Freenet kernel software and
connected to the network. Peers are organized in a ring formation, with each
position on the ring representing a value between 0.0 and 1.0. This value
corresponds to the peer's location within the network.

## Neighbor Connections

Each peer is connected to a number of other peers, known as its neighbors. Peers
establish bi-directional UDP connections to each other, using firewall traversal
methods when necessary. These connections are chosen to ensure the network
maintains a small-world topology. This means that most of a peer's connections
will be to peers close to them, but with occasional long-distance connections.

## Adaptive Routing

When a peer wishes to read, create, or modify a contract it must send a suitable
request to the peers hosting the contract, which it can do by sending it to
whichever neighbor is most likely to be able to retrieve the contract quickly.

All else being equal this will be the neighbor closest to the contract's
location ("greedy routing"), but in reality other factors will pay a role. For
example, if a peer is on a slower connection.

To address this Freenet will monitor peer's past performance and select the peer
most likely to respond successfully the fastest, considering both past
performance and distance from the desired contract. This is known as adaptive
routing and relies on an algorithm called [isotonic
regression](https://github.com/sanity/pav.rs).