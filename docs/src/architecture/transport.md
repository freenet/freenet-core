# Freenet Transport Protocol (FrTP)

## Introduction

The Freenet Transport Protocol (FrTP) is a UDP-based system designed to ensure reliable and encrypted
message transmission. This document outlines the key elements of FTP, including connection 
establishment, message handling, and rate limiting.

## Overview

* **Firewall Traversal**: FrTP allows peers behind firewalls to establish direct connections.
* **Security**: All messages are encrypted using AES128GCM, with RSA public key exchange for
  connection establishment, should effectively thwart man-in-the-middle attacks.
* **Streaming**: Large messages can be streamed, meaning that a peer can start forwarding data
  before the entire message is received.
* **Covert**: FrTP can run on any UDP port and FrTP packets look like random data, although more
   sophisticated analysis of packet timing and size could be used to identify FrTP traffic.
* **Efficient**: FrTP is designed to minimize bandwidth usage, with rate limiting and message
  batching.

## Connection Establishment

### Scenario 1: Both Peers Behind NAT

From Peer A's perspective (also applicable to Peer B as protocol is symmetric):

1. **Initial Knowledge**: Each peer must know the other's IP, port, and RSA public key.
2. **Key Generation**: Peer A generates a random AES128GCM symmetric key,
   termed `outbound_symmetric_key`.
3. **Outbound Hello Message**: Peer A encrypts `outbound_symmetric_key` and a u16 protocol version
   number with Peer B's
   public key, sending this as the `outbound_hello_message`.
    - **Note**: This is the sole message type not encrypted with `outbound_symmetric_key`.
4. **Inbound Hello Message**: Peer A awaits Peer B’s `inbound_hello_message`, decrypts it using its
   private key, and
   stores the `inbound_symmetric_key` in `UdpConnectionInfo`.
5. **Acknowledgement Protocol**: Peer A repeatedly sends `outbound_hello_message` every 200ms until
   a `hello_ack` from Peer B is received or a 5-second timeout occurs, indicating connection 
   failure.
6. **Hello Acknowledgement**: Upon receiving `inbound_hello_message`, Peer A sends back `hello_ack`,
   using `outbound_symmetric_key`.
    - **Repeated Hello Handling**: If a repeated `inbound_hello_message` is detected, Peer A
      retransmits `hello_ack` and disregards the duplicate message.
7. **Connection Establishment**: Connection is established once both peers have exchanged and
   acknowledged `hello_ack` messages.

### Scenario 2: Peer A Behind NAT, Peer B as Gateway

Here, Peer A initiates the connection, while Peer B operates as a gateway.

1. **Symmetric Key Selection**: Peer A generates an AES128 symmetric key, serving as
   both `outbound_symmetric_key` and `inbound_symmetric_key`.
2. **Outbound Hello Message**: Similar to Scenario 1, Peer A sends an encrypted symmetric key to
   Peer B.
3. **Acknowledgement and Connection**: Peer B decrypts the message, acknowledges it, and establishes
   the connection using the symmetric key.

## Keep-Alive Protocol

To maintain an open connection, `keep_alive` messages are exchanged every 30 seconds. A connection 
is terminated if a peer fails to receive any message within 120 seconds.

## Symmetric Message Schema

```rust
pub struct SymmetricMessage {
    pub message_id: u16,
    // Unique number for message tracking and duplicate detection
    pub confirm_receipt: Vec<u16>,
    // Confirmation mechanism to identify dropped messages
    pub payload: SymmetricMessagePayload,
}

pub enum SymmetricMessagePayload {
    AcknowledgeHello { encrypted_symmetric_key: Vec<u8> },
    NoOperation,
    KeepAlive {
        /// Arbitrary data peers can share with their neighbors
        metadata: HashMap<String, String> 
    },
    Disconnect,
    ShortMessage { payload: Vec<u8> },
    LongMessageFragment {
        total_length: u64,
        start_index: u64,
        payload: Vec<u8>,
    },
}
``` 

## Message Handling

### Dropped and Out-of-Order Messages

- **Duplicate Detection**: Messages are checked for duplicate `message_id` and hash. Duplicates
  trigger an immediate `NoOperation` message with a reconfirmation in `confirm_receipt`.
- **Acknowledgement Timeout**: Messages are resent if not acknowledged within 2 seconds 
  (`MESSAGE_CONFIRMATION_TIMEOUT`).

### Confirmation Batching

- **Batching Strategy**: Receipts can be delayed up to 500ms (`MAX_CONFIRMATION_DELAY`) to enable
  batch confirmation.
- **Queue Management**: Receipt queues exceeding 20 messages prompt immediate confirmation to
  prevent overflow.

## Message Types

- **Short Messages**: Contained within a single UDP packet (up to 1kb).
- **Long Messages**: Split into fragments for larger payloads, enabling efficient data forwarding.

## Rate Limiting

- **Initial Setup**: Upstream bandwidth set 50% above desired usage to allow for traffic bursts.
- **Dynamic Adjustment**: Future adaptations may use isotonic regression for optimizing bandwidth
  and packet loss
  balance.
- **Implementation**: Bandwidth monitoring over 10-second windows (`BANDWIDTH_MEASUREMENT_WINDOW`).
  Exceeding limits
  triggers a 10ms sleep (`BANDWIDTH_CONTROL_SLEEP_DURATION`), with periodic reassessment.

## Conclusion

The Freenet Transport Protocol provides a robust framework for secure and efficient data
transmission. Its design considers NAT challenges, message integrity, and bandwidth management, 
ensuring reliable communication in various network conditions.