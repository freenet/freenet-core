# Keep-alive Timeout Investigation - Hypotheses

## Problem Statement
Connections timeout after exactly 30 seconds despite v0.1.11 async WASM fix. The pattern shows:
- Connections establish successfully
- Work for ~30 seconds
- Timeout and reconnect in endless cycle
- Technic gateway sends repeated intro packets even after ACK

## Hypotheses (in order of likelihood)

### 1. Keep-alive task is being cancelled/dropped after 30 seconds
**Evidence For:** 
- Connections work fine for first 30 seconds
- Timeout happens at exactly 30 seconds (KILL_CONNECTION_AFTER constant)
**Evidence Against:** 
- Keep-alive task is STILL RUNNING when timeout occurs
- ERROR log confirms: "Keep-alive task is STILL RUNNING despite timeout!"
**Status:** REFUTED ❌
**Test:** Add logging for keep-alive task lifecycle (creation, sending, cancellation)

### 2. Keep-alive packets are sent but not received/processed
**Evidence For:**
- Vega: Sending keep-alives every 10s but receiving ZERO
- Technic: Sending many keep-alives, receiving only some
- Connection times out after exactly 30 seconds of no received packets
**Evidence Against:**
- None
**Status:** CONFIRMED ✓
**Test:** Add logging for both sending AND receiving keep-alive packets with timestamps

### 3. Keep-alive packets are using wrong encryption keys
**Evidence For:**
- Technic gateway shows repeated intro packet attempts with decryption errors
**Evidence Against:**
- Initial connection works fine for 30 seconds
**Status:** Unconfirmed
**Test:** Log encryption keys used for keep-alive packets vs regular packets

### 4. Channel overflow is causing keep-alive packets to be dropped
**Evidence For:**
- Previous investigation showed channel overflow issues
**Evidence Against:**
- Channel overflow was more related to WASM blocking
**Status:** Unconfirmed
**Test:** Add channel capacity monitoring for keep-alive send channel

### 5. The persistent keep-alive timer has a bug in its interval calculation
**Evidence For:**
- None
**Evidence Against:**
- Intervals are exactly 10 seconds (9999-10000ms)
- Timer is working correctly
**Status:** REFUTED ❌
**Test:** Log actual intervals between keep-alive attempts

### 6. Network/firewall issue preventing packets from vega
**Evidence For:**
- ZERO packets received from vega (not even decryption errors)
- Technic (same LAN) works partially
- Vega is on AWS EC2 which has security groups
**Evidence Against:**
- Initial connection works (otherwise wouldn't establish at all)
**Status:** Likely
**Test:** Check AWS security group configuration

## Next Steps
1. Add comprehensive logging to test each hypothesis
2. Run tests and collect evidence
3. Update hypothesis status based on findings