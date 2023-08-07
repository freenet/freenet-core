# Delegates

Just as human delegates are representatives who make decisions or take action on
behalf of others, in Freenet, Delegates serve a similar role. They function like
an advanced version of a web browser's local storage while sharing similarities
with Unix "Daemons". They run in the background on your device within the
Freenet kernel, providing a secure environment for managing private data and
performing complex tasks.

Delegates interact with various Freenet components, including Contracts, User
Interfaces, and other Delegates. They securely store your private data,
including cryptographic keys, tokens, and passwords. They can read and modify
data stored in Contracts, or send and receive messages from other Delegates and
UIs. Delegates can also communicate directly with the user to, for example, ask
permission to perform a task or notify the user of an event.

Delegates are WebAssembly code and must comply with the
[DelegateInterface](https://github.com/freenet/locutus/blob/b1e59528eaeba31c7f09881594d19347de60e8cd/crates/locutus-stdlib/src/delegate_interface.rs#L121)
trait.

## Actor Model and Message Passing

Delegates utilize a message passing system similar to the [actor
model](https://en.wikipedia.org/wiki/Actor_model) to interact with Contracts,
other Delegates, and Applications. 

The Freenet kernel makes sure that for any incoming message, whether it's from
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