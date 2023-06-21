## Freenet Kernel

The kernel is the core of Freenet, it's the software that runs on the user's
computer. It's responsible for:

* Providing a user-friendly interface to access Freenet via a web browser
* Host the user's [delegates](delegates.md) and the private data they store
* Host [contracts](contracts.md) and their associated data on behalf of the network
* Manage communication between contracts, delegates, and UI components

The kernel is written in Rust and is designed to be small (hopefully less than 5
MB), efficient, and able to run on a wide range of devices like smartphones, desktop
computers, and embedded devices.
