# Components

## Low-level transport

Establish direct connections between Freenet nodes, performing NAT hole-punching where
necessary. These connections will be encrypted to prevent snooping, and endpoints will be
verified to prevent main-in-the-middle attacks. 

Support the seconding of short messages over these connections, and also the streaming
of data to allow the transmission of larger data.

Provide a convenient interface to the low-level transport layer using struct serialization
and a callback mechanism for message responses to ensure the clarity and simplicity of
calling code.

## Key-Value store



## Contract Specification and API

Cryptographic contracts specified in WebAssembly determine what are valid values for
a given key/contract, 
