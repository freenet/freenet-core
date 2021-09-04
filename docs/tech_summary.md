# Freenet 2 technical summary

## Goal

Freenet 2 will allow the creation of diverse Internet applications that are entirely decentralized, 
and depend only on publicly verifiable cryptographic contracts rather than highly centralized 
third party infrastructure providers.

## Implementation

F2 is an entirely decentralized key-value store with [observer semantics](https://en.wikipedia.org/wiki/Observer_pattern), 
where keys are cryptographic contracts that specify valid values for the key and valid updates to values for the key.

Decentralization and scalability is achived through a [small world network](https://en.wikipedia.org/wiki/Small-world_network).

