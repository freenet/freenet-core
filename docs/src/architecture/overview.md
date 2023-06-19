# Freenet Architecture Overview

Delegates, contracts, and user interfaces (UIs) each serve distinct roles in the
Freenet ecosystem. [Contracts](contracts.md) control public data, or "shared
state". [Delegates](delegates.md) act as the user's agent and can store private
data on the user's behalf, while [User Interfaces](ui.md) provide an interface
between these and the user through a web browser. UIs are distributed through
the P2P network via contracts.

![Architectural Primitives Diagram](components.svg)

## Freenet Kernel

The kernel is the core of Freenet, it's the software that runs on the user's
computer. It's responsible for:

* Providing a user-friendly interface to access Freenet via a web browser
* Host the user's [delegates](#delegates) and the private data they store
* Host [contracts](#contracts) and their associated data on behalf of the network
* Manage communication between contracts, delegates, and UI components

The kernel is written in Rust and is designed to be small (hopefully less than 5
MB), efficient, and able to run on a wide range of devices like smartphones, desktop
computers, and embedded devices.

