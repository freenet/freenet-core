## Delegates

Delegates are WebAssembly code that act as user avatars on Freenet, they
must implement the [DelegateInterface](https://github.com/freenet/locutus/blob/b1e59528eaeba31c7f09881594d19347de60e8cd/crates/locutus-stdlib/src/delegate_interface.rs#L121).

Delegates run in the Freenet kernel and manage private data and interact with
other Freenet entities like contracts, apps, and other delegates on behalf of
the user. They can store and control private data like cryptographic keys,
tokens, and passwords, and communicate with users, for example to ask permission
to sign some data. They can be created by UI components or other delegates.

Unlike contracts, which require network verification, delegates run on the
user's computer and are trusted to execute code without verification. Delegates'
state is private, while contracts' state is public but may be encrypted.

Delegates are used for:
- Managing private data similar to a browser's [web
  storage](https://en.wikipedia.org/wiki/Web_storage)
  - eg. private keys, tokens
- Acting on the user's behalf on Freenet
  - eg. consuming received messages in an inbox
- Storing user data
  - e.g., contacts, messages

### Security Model: Sender Attestation

Delegates utilize a messaging system akin to the [actor
model](https://en.wikipedia.org/wiki/Actor_model) to interact with contracts,
other delegates, and applications. Regardless of whether the message originates
from another delegate, a User Interface (UI), or as a state update from a
contract, the Freenet kernel ensures that the recipient can verify the sender's
identity for any incoming message. This is known as "sender attestation".

### Delegate Use Cases

* A key manager delegate is responsible for managing a user's private keys,
  other components can request that the key manager delegate sign messages
  or other data on their behalf.

* An inbox delegate is responsible for maintaining an inbox of messages sent to
  the user in a messaging system. It pulls messages from an inbox contract,
  decrypts them, and stores them locally where they can be queried by other
  components.
