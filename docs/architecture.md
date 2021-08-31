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

Retrieve a contract's value. The contract webassembly is hashed, and the hash is 
[converted](https://github.com/sanity/locutus/blob/master/src/main/kotlin/locutus/tools/math/Location.kt#L23) 
to a location on the ring. The Get request is then "greedy routed" towards that
location. If/when the data is found it is returned along the same path to
the requestor, potentially being cached by peers along the way.

##### Caching

Peers will cache the closest data to their location, or the most requested data
once this can be observed. We will need to determine how to balance these two
overlapping goals. Peers subscribe to updates for the data they cache.

#### Put

Set or update the value of a contract. The put request is greedy routed to the
location of the contract on the ring. If a peer receives a put request and
it is the closest peer to that location that it's aware of it will cache it.
If a peer receives a put request for data other peers are subscribed to, it
will broadcast to those peers.

#### Subscribe

Listen for changes to a contract's value
