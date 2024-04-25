# Delegates

In Freenet, Delegates are software components that can act on the user's behalf. 
Think of them as a more sophisticated version of a web browser's local storage, 
with similarities to Unix "Daemons". Operating within the Freenet core on your 
device, Delegates are a secure and flexible mechanism for managing private data, 
such as cryptographic keys, tokens, and passwords, and executing complex tasks.

Delegates interact with various components within Freenet, including Contracts,
User Interfaces, and other Delegates. They can also communicate directly with
the user, such as to request user permissions or notify the user of events.

Implemented in WebAssembly and adhering to the
[DelegateInterface](https://github.com/freenet/freenet-core/blob/b1e59528eaeba31c7f09881594d19347de60e8cd/crates/freenet-stdlib/src/delegate_interface.rs#L121)
trait, Delegates seamlessly integrate within the Freenet network, operating
securely on your devices.

## Actor Model and Message Passing

Delegates communicate with Contracts, other Delegates, and UIs by passing
messages, similar to the [actor
model](https://en.wikipedia.org/wiki/Actor_model).

The Freenet Core makes sure that for any incoming message, whether it's from
another Delegate, a User Interface, or a Contract update, the receiver knows who
the sender is. This allows delegates to verify the behavior of any component
they interact with, and decide if they can be trusted.

## Delegate Use Cases

Delegates have a wide variety of uses:

- A **key manager delegate** manages a user's private keys. Other components can
  request that this Delegate sign messages or other data on their behalf.

- An **inbox delegate** maintains an inbox of messages sent to the user in an
  email-like system. It retrieves messages from an inbox Contract, decrypts
  them, and stores them locally where they can be accessed by other components
  like a user interface.

- A **contacts delegate** manages a user's contacts. It can store and retrieve
  contact information, and can be used by other components to send messages to
  contacts.

- An **alerts delegate** watches for events on the network, such as a mention
  of the user's name in a discussion, and notifies the user of these events
  via an alert.

Moreover, Delegates can securely synchronize with identical Delegate instances
running on other devices controlled by the user, such as a laptop, phone, or
desktop PC. This synchronization, facilitated through a shared secret private
key provided by the user, allows the Delegates to communicate securely, acting
as both backups and replicas of each other through Freenet's peer-to-peer
network.

## Similarity to Service Workers

Delegates have much in common with [Service Workers](https://developer.mozilla.org/en-US/docs/Web/API/Service_Worker_API) in
the web browser ecosystem. Both are self-contained software modules, running
independently of the user interface and performing complex tasks on behalf of
the user.

However, Delegates are even more powerful. While Service Workers can
store data and interact with components within the scope of the web browser and
its pages, Delegates can talk to other Delegates in the same device, or with
other Delegates running elsewhere via Freenet's peer-to-peer network.
