# Delegates

Freenet's Delegates are advanced, self-contained software modules, which act as
user agents within the Freenet ecosystem. They can perform complex tasks under a
strict security model, managing private data and interacting with other Freenet
[components](overview.md) like contracts, user interfaces, and other delegates.

Implemented in WebAssembly, they adhere to the
[DelegateInterface](https://github.com/freenet/locutus/blob/b1e59528eaeba31c7f09881594d19347de60e8cd/crates/locutus-stdlib/src/delegate_interface.rs#L121)
and run directly within the Freenet kernel on the user's device, trusted to
carry out their tasks without external verification.

Delegates can control and store private data, such as cryptographic keys,
tokens, and passwords. They can also be created by UI components or other
delegates. They perform actions on the user's behalf on Freenet, like consuming
received messages in an inbox, or storing user data like contacts and messages.

Delegates protect or "encapsulate" their stored private data. For instance, a
delegate responsible for a private key would be asked to sign a document by
another component, instead of having to reveal the key itself. This encapsulation
ensures that the private key is never exposed to other components.

## Actor Model and Message Passing

Delegates utilize a message passing system similar to the [actor
model](https://en.wikipedia.org/wiki/Actor_model) to interact with contracts,
other delegates, and applications. 

The Freenet kernel makes sure that for any incoming message, whether it's from
another delegate, a user interface, or a contract update, the receiver knows who
the sender is. This allows delegates to verify the behavior of any component
they interact with, and decide if they can be trusted.

## Delegate Use Cases

Delegates have a wide variety of uses:

- A **key manager delegate** manages a user's private keys. Other components can
  request that this delegate sign messages or other data on their behalf.

- An **inbox delegate** maintains an inbox of messages sent to the user in an
  email-like system. It retrieves messages from an inbox contract, decrypts
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

However, delegates are even more powerful. While Service Workers can
store data and interact with components within the scope of the web browser and
its pages, delegates can talk to other delegates in the same device, or with
other delegates running elsewhere via Freenet's peer-to-peer network.