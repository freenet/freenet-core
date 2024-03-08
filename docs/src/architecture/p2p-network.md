# Freenet Network Topology

## Small-World Network

Freenet is structured as a decentralized peer-to-peer network, utilizing the
principles of a small-world network. This unique network topology enhances
Freenet's robustness against denial-of-service attacks, enables it to scale
dynamically based on user demand, and facilitates efficient and transparent data
storage. Additionally, users can subscribe to specific data keys to receive
immediate notifications about any updates.

![Small World Network](p2p-network.svg)

## Understanding Freenet Peers

In Freenet, a "peer" is any computer running the Freenet Core software. The 
peers are organized in a ring-like structure, with each peer assigned a 
specific numerical value between 0.0 and 1.0, indicating its location in the 
network's topology. This location is derived from the peer's public key.

## Establishing Neighbor Connections

Every Freenet peer, also referred to as a node, forms two-way connections with a
set of other peers, termed "neighbors." These connections utilize the User
Datagram Protocol (UDP) and can do [Frewall hole punching](https://en.wikipedia.org/wiki/Hole_punching_(networking)) when necessary. Peers manage their resource usage —
bandwidth, memory, CPU, and storage — based on limits set by the user.

## Adaptive behavior

Peers keep track of their neighbor's performance and learn to prefer faster
connections over time.

Peers can also identify bad behavior by other peers like excess resource usage and 
will disconnect from them.
