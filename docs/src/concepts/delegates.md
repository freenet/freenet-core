# Delegates

Freenet's Delegates are advanced, self-contained software modules, which act as
user agents within the Freenet ecosystem. They can perform complex tasks
under a strict security model, managing private data and
interacting with other Freenet entities like contracts, apps, and other
delegates on the user's behalf.

Implemented in WebAssembly, they adhere to the
[DelegateInterface](https://github.com/freenet/locutus/blob/b1e59528eaeba31c7f09881594d19347de60e8cd/crates/locutus-stdlib/src/delegate_interface.rs#L121)
and run directly within the Freenet kernel on the user's device, trusted to
carry out their tasks without external verification.

Delegates can control and store private data, such as cryptographic keys,
tokens, and passwords. They can also be created by UI components or other
delegates. They perform actions on the user's behalf on Freenet, like consuming
received messages in an inbox, or storing user data like contacts and messages.

Like a much more powerful version of a web browser's [web
storage](https://en.wikipedia.org/wiki/Web_storage), Delegates offer a higher
level of discernment and trust. For instance, a Delegate tasked with managing a
private key could be asked to sign a document with that key. However, the
decision to do so may depend on the identity of the requester.

## Actor Model and Message Passing

Delegates utilize a message passing system similar to the [actor
model](https://en.wikipedia.org/wiki/Actor_model) to interact with contracts,
other delegates, and applications. Regardless of whether the message originates
from another delegate, a User Interface, or as a state update from a contract,
the Freenet kernel ensures that the recipient can verify the sender's identity
for any incoming message. This feature enables delegates to verify the behavior
of any component they interact with, and to decide whether to trust them or not.

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
