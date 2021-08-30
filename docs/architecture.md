# Architecture Layers

## Transport

Communication between peers is handled by libp2p, this includes:

* Establishing direct P2P connection between peers, through NATs if necessary
* Encryption of communication
* Reliable transmission of data between peers

## Small World Ring

Establishes and maintain connection to other peers in a small-world ring, allowing
messages to be routed to peers based on their location in the ring. Maintains
small world topology as peers are disconnected and join.

### Joining

A new peer joins the network via a gateway, an "open" peer that accepts unilateral
connections and isn't behind a NAT. The joining peer's network location is negotiated
between that peer and the gateway such that neither can choose the new peer's location.

### Load balancing

Peers track how much CPU and bandwidth they use on behalf of other peers. Peers may
disconnect from other peers if they are consuming too many resources. Karma (see below)
may be used to establish a reputation initially.

## Contract Store

### Key/value store

Store and retrieve keys and associated data, perhaps using SQLite implementing a 
least-recently-used eviction policy.

### Network operations

#### Get

Retrieve a contract's value

#### Put

Set or update the value of a contract

#### Listen

Listen for changes to a contract's value

## Karma

Karma is a scarce unit of value which can be used to establish trust within the network.
Karma can be acquired through a donation to Freenet development.

Notes:
* Blind signature may be used to purchase Karma without it being tied to a real-world
  transaction.