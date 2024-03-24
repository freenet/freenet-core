# Components of Decentralized Software

Delegates, contracts, and user interfaces (UIs) each serve distinct roles in the
Freenet ecosystem. [Contracts](contracts.md) control public data, or "shared
state". [Delegates](delegates.md) act as the user's agent and can store private
data on the user's behalf, while [User Interfaces](ui.md) provide an interface
between these and the user through a web browser. UIs are distributed through
the P2P network via contracts.

![Architectural Primitives Diagram](components.svg)

## Freenet Core

The Freenet Core is the software that enables a user's computer to connect to
the Freenet network. Its primary functions are:

* Providing a user-friendly interface to access Freenet via a web browser
* Host the user's [delegates](delegates.md) and the private data they store
* Host [contracts](contracts.md) and their associated data on behalf of the
  network
* Manage communication between contracts, delegates, and UI components

Built with Rust, the core is designed to be compact (ideally under 5 MB),
efficient, and capable of running on a variety of devices such as smartphones,
desktop computers, and embedded devices.
