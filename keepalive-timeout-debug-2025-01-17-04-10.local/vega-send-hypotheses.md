# Vega Packet Send Investigation - Hypotheses

## Problem Statement
Application logs show "Keep-alive NoOp packet sent successfully" but tcpdump shows NO packets leaving the network interface.

## Hypotheses (in order of likelihood)

### 1. Wrong destination address in send()
**Theory:** Gateway might be sending to wrong IP/port (e.g., internal address instead of external)
**Test:** Log the exact destination address in send() call
**Evidence For:** Gateway uses different addressing than regular peers
**Evidence Against:** Initial connection works

### 2. Socket send() returns success but packet is dropped by kernel
**Theory:** Send buffer full, MTU issues, or other kernel-level drop
**Test:** Check send() return value and errno, log packet size
**Evidence For:** No error logged despite packet not leaving
**Evidence Against:** Usually send() would return error

### 3. Using wrong socket or socket not properly configured
**Theory:** Gateway might have multiple sockets, sending on wrong one
**Test:** Log socket fd and configuration when sending
**Evidence For:** Gateway mode has special socket handling
**Evidence Against:** Socket is bound (ss shows it listening)

### 4. Packet corruption causing kernel to drop
**Theory:** Malformed packet structure causes silent drop
**Test:** Log packet contents and size before send
**Evidence For:** Continuous decryption errors suggest protocol mismatch
**Evidence Against:** Application layer shouldn't affect packet validity

### 5. Gateway-specific send path has a bug
**Theory:** Special gateway code path that bypasses normal send
**Test:** Trace the code path for gateway vs normal peer sends
**Evidence For:** Only affects gateway, not regular peers
**Evidence Against:** Same code works on technic gateway

## Logging Points Needed
1. In outbound_packets.send() - log destination address
2. In actual socket send() - log return value, errno, packet size
3. Socket configuration - log fd, bind address, socket options
4. Packet contents - first 32 bytes of packet data
5. Gateway vs peer send path - which code branch is taken