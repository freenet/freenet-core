# Concepts Overview

Delegates, contracts, and user interfaces (UIs) each serve distinct roles in the
Freenet ecosystem. [Contracts](contracts.md) control public data, or "shared
state". [Delegates](delegates.md) act as the user's agent and can store private
data on the user's behalf, while [User Interfaces](ui.md) provide an interface
between these and the user through a web browser. UIs are distributed through
the P2P network via contracts.

All of this occurs within the [Freenet Kernel](kernel.md), the software
that runs on the user's computer so it can join the Freenet network.

![Architectural Primitives Diagram](components.svg)
