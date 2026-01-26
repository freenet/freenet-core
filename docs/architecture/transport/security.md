# Transport Layer Security Architecture

## Overview

The Freenet transport layer uses a hybrid cryptographic approach combining elliptic curve key exchange with authenticated encryption. The security model prioritizes:

1. **Peer identity** - Public keys serve as node identifiers
2. **Confidentiality** - All packet payloads are encrypted
3. **Authenticity** - AEAD tags prevent tampering
4. **Simplicity** - Straightforward protocol without complex state machines

## Cryptographic Protocol

### Two-Phase Approach

**Phase 1: Connection Establishment (Asymmetric)**
- X25519 elliptic curve Diffie-Hellman for key exchange
- ChaCha20Poly1305 AEAD for intro packet encryption
- Establishes symmetric keys for both directions

**Phase 2: Data Transfer (Symmetric)**
- AES-128-GCM authenticated encryption for all data packets
- Counter-based nonces for performance
- Independent keys for each direction

**Rationale:** Asymmetric crypto only during handshake keeps overhead low while enabling public-key-based peer identity.

### Protocol Flow

```
Alice                                                Bob
  |                                                   |
  | 1. Generate ephemeral X25519 keypair             |
  | 2. Derive shared secret with Bob's public key    |
  | 3. Encrypt handshake with ChaCha20Poly1305       |
  | ───────────────────────────────────────────────> |
  |    [ephemeral_pub | encrypted(symmetric_key_A)]  |
  |                                                   | 4. Decrypt with static secret key
  |                                                   | 5. Extract Alice's symmetric key
  |                                                   | 6. Generate Bob's symmetric key
  | <─────────────────────────────────────────────── |
  |    encrypted_with_key_A(symmetric_key_B)         |
  | 7. Both sides now have bidirectional keys        |
  | <──────────────────────────────────────────────> |
  |    All subsequent packets use AES-128-GCM        |
```

**Code Reference:** `crates/core/src/transport/crypto.rs`, `connection_handler.rs`

## Security Properties

### What Is Protected

✅ **Confidentiality**
- All packet payloads encrypted
- Symmetric keys never transmitted in cleartext
- Handshake data encrypted with ephemeral keys

✅ **Authentication**
- AEAD tags on every packet (128-bit)
- Intro packets prove possession of private key
- Failed authentication → packet dropped

✅ **Integrity**
- Tampering detected via AEAD tags
- Bit flips, truncation, injection all caught
- No silent corruption possible

✅ **Peer Identity**
- Public key = node identity
- Cryptographic binding between identity and traffic
- No separate authentication layer needed

### What Is NOT Protected

❌ **Traffic Analysis**
- Packet sizes visible
- Timing patterns observable
- Connection metadata in cleartext
- **Design choice:** Freenet focuses on content privacy, not traffic pattern obfuscation

❌ **Forward Secrecy**
- Static keys used (no session key rotation)
- Compromise of long-term key reveals past traffic
- **Trade-off:** Simplicity and performance vs. PFS

❌ **Replay Protection (Limited)**
- Nonce uniqueness prevents exact packet replay
- No explicit sequence numbers or replay counters
- **Mitigation:** Application-level transaction IDs provide replay detection

## Threat Model

### Mitigated Threats

| Threat | Mitigation | Status |
|--------|-----------|--------|
| Passive eavesdropping | Encryption | ✅ Protected |
| Active MITM | AEAD authentication | ✅ Protected |
| Identity spoofing | Public key authentication | ✅ Protected |
| Packet tampering | AEAD tags | ✅ Protected |
| DoS (crypto exhaustion) | Rate limiting | ✅ Mitigated |

### Out-of-Scope Threats

| Threat | Status | Reason |
|--------|--------|--------|
| Traffic analysis | ❌ Not mitigated | Design decision: focus on content privacy |
| Forward secrecy | ❌ Not provided | Simplicity > PFS for this use case |
| Long-term key compromise | ❌ Limited protection | No session ratcheting |
| Replay attacks | ⚠️ Partially mitigated | Nonce uniqueness only, no sequence validation |

**Design Philosophy:** Freenet prioritizes **correct implementation of core security properties** over comprehensive protection against all theoretical threats. The threat model is appropriate for a distributed content network where availability and performance matter.

## Key Management

### Key Generation

**Node Identity Keys:**
- Generated once per node lifetime
- X25519 keypair (32-byte secret, 32-byte public)
- Stored in node configuration directory
- Public key serves as network identifier

**Session Keys:**
- Generated per connection during handshake
- 16-byte AES-128 keys (128-bit security level)
- Independent keys for each direction
- Ephemeral (lost on connection close)

**Rationale:** Static node identity enables persistent peer relationships; ephemeral session keys provide per-connection isolation.

**Code Reference:** `crates/core/src/transport/crypto.rs:92-103` (key generation)

### Key Lifecycle

1. **Generation:** Random bytes from `GlobalRng` (deterministic in tests, secure in production)
2. **Storage:** Static keys persisted, session keys in memory only
3. **Use:** X25519 static keys for handshake, AES-128 session keys for data
4. **Destruction:** Session keys cleared on connection close, static keys zeroized on drop

### Key Compromise Scenarios

**Static Key Compromise:**
- Attacker can decrypt past traffic (no forward secrecy)
- Attacker can impersonate compromised node
- Mitigation: Rotate node identity (manual process, requires network re-announcement)

**Session Key Compromise:**
- Attacker can decrypt single connection
- No impact on other connections (key isolation)
- Attacker cannot derive static keys from session keys

## Implementation Details

### Nonce Strategy

**Problem:** AES-GCM requires unique nonces per (key, nonce) pair.

**Solution:** Counter-based approach for performance:
- 4-byte random prefix (generated once per connection)
- 8-byte atomic counter (incremented per packet)
- Total: 12-byte nonce

**Performance:** ~5.5× faster than random nonce generation

**Safety:** Unique as long as counter doesn't wrap (2^64 packets = not feasible)

**Code Reference:** `crates/core/src/transport/packet_data.rs:28-70`

### DoS Protection

**Rate Limiting on Asymmetric Operations:**
- Minimum 1 second between intro packet decryption attempts per source IP
- Prevents crypto exhaustion attacks
- Expired entries cleaned up every 60 seconds

**Purpose:** X25519 operations are more expensive than symmetric crypto, so rate limiting prevents attackers from overwhelming the node with handshake attempts.

**Code Reference:** `crates/core/src/transport/connection_handler.rs:47-50, 654-668`

### Protocol Versioning

**Version Enforcement:**
- Protocol version included in encrypted handshake
- Mismatch causes connection rejection
- Prevents downgrade attacks

**Version Format:**
- 8 bytes: `[major:1][minor:1][patch:1][flags:4][reserved:1]`
- Flags encode pre-release tags (alpha/beta/rc)

**Upgrade Strategy:** Major version mismatch → incompatible, minor version mismatch → backward compatible

**Code Reference:** `crates/core/src/transport/connection_handler.rs:1940-2058`

## Algorithm Selection

### Why X25519?

**Previous:** RSA-2048
**Current:** X25519

**Reasons for change:**
1. **Performance:** ~100× faster than RSA-2048
2. **Packet size:** 32 bytes vs. 256 bytes (intro packets: 73 bytes vs. ~300 bytes)
3. **Security:** Modern ECC vs. legacy RSA
4. **Standard:** Well-vetted, widely used (TLS 1.3, WireGuard)

**Trade-off:** Slightly different security assumptions (ECDLP vs. factoring), but X25519 is considered state-of-the-art.

### Why ChaCha20Poly1305 for Handshake?

- Natural pairing with X25519 (standard in Noise protocol)
- Fast software implementation
- AEAD provides authentication + encryption

### Why AES-128-GCM for Data?

- Hardware acceleration on modern CPUs (AES-NI)
- Industry standard for symmetric encryption
- 128-bit security sufficient for distributed storage
- AEAD provides integrity protection

### Why Not More Exotic Primitives?

**Philosophy:** Use battle-tested, widely deployed cryptography. The security of Freenet depends on correct implementation of simple primitives, not on novel algorithms.

## Security Testing

### Automated Tests

- **Round-trip encryption** - Encrypt/decrypt cycles
- **Tampering detection** - Modified packets rejected
- **Wrong key detection** - Mismatched keys fail cleanly
- **Protocol version enforcement** - Incompatible versions rejected

**Test Coverage:** Core cryptographic operations covered, protocol-level fuzzing would be beneficial addition.

**Code Reference:** `crates/core/src/transport/crypto.rs` (test module), `packet_data.rs` (test module)

### Future Improvements

**Potential Enhancements (NOT Roadmap):**
1. **Forward secrecy:** Implement session key rotation using ratcheting
2. **Replay protection:** Add explicit sequence numbers with sliding window
3. **Traffic obfuscation:** Padding to obscure message sizes
4. **Key rotation:** Automated long-term key migration
5. **Security audit:** Third-party review of protocol and implementation

**Note:** These are speculative ideas for discussion, not committed features.

## Comparison to Other Protocols

### vs. TLS 1.3

**Similarities:**
- Modern crypto (X25519, AES-GCM, ChaCha20Poly1305)
- AEAD for data packets
- Public key authentication

**Differences:**
- TLS: Forward secrecy via ephemeral keys + ratcheting
- Freenet: Simpler protocol, static keys
- TLS: Certificate infrastructure
- Freenet: Public key = identity

**Why not use TLS:** UDP-based transport, different trust model (no CA), performance requirements, deterministic testing needs.

### vs. Noise Protocol

**Similarities:**
- X25519 + ChaCha20Poly1305 handshake pattern
- Static + ephemeral key exchange
- Symmetric encryption for data

**Differences:**
- Noise: Formal framework with many patterns
- Freenet: Custom simplified variant
- Noise: Session state machines
- Freenet: Simpler stateless approach

**Why not use Noise directly:** Slightly different requirements (UDP framing, protocol versioning), desire for minimal dependencies, ability to optimize for Freenet's specific use case.

## Operational Considerations

### Key Backup and Recovery

**Static Node Keys:**
- Critical for node identity persistence
- Should be backed up securely
- Loss → node must rejoin network with new identity

**Recommendation:** Encrypt backups, store offline, test recovery procedures.

### Security Monitoring

**Metrics to Monitor:**
- Failed decryption rate (indicates tampering or key mismatch)
- Intro packet rate per IP (DoS detection)
- Connection rejections due to version mismatch
- Unexpected disconnections (may indicate MITM attempts)

**Normal vs. Suspicious:**
- Normal: Occasional failed decryption (network errors, restarts)
- Suspicious: Sustained high rate from single IP or many IPs

**Code Reference:** `crates/core/src/transport/metrics.rs` (telemetry integration)

### Incident Response

**Suspected Key Compromise:**
1. Generate new node identity keys
2. Shutdown node gracefully
3. Replace keys in configuration
4. Restart node (will rejoin with new identity)
5. Monitor for suspicious connection attempts to old identity

**Network-wide Vulnerability:**
1. Coordinate version upgrade
2. Deploy patched nodes
3. Monitor adoption rate
4. Deprecate vulnerable versions

## References

### Standards and RFCs

- RFC 7539 - ChaCha20-Poly1305 AEAD
- RFC 5869 - HKDF (key derivation, used in X25519 implementation)
- NIST SP 800-38D - AES-GCM mode

### Implementations

- `curve25519-dalek` - X25519 implementation
- `chacha20poly1305` crate - AEAD construction
- `aes-gcm` crate - AES-GCM implementation

### Source Code

| Component | Location |
|-----------|----------|
| Cryptographic primitives | `crates/core/src/transport/crypto.rs` |
| Handshake protocol | `crates/core/src/transport/connection_handler.rs` |
| Packet encryption/decryption | `crates/core/src/transport/packet_data.rs` |
| Symmetric messages | `crates/core/src/transport/symmetric_message.rs` |
| DoS rate limiting | `crates/core/src/transport/connection_handler.rs:47-50, 654-668` |

---

**Last Updated:** 2026-01-19
**Status:** Production (X25519 + ChaCha20Poly1305 + AES-GCM)
