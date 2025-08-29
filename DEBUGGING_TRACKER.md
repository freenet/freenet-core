# Freenet Core Debugging Tracker

## Current Focus: River End-to-End Integration Issues

### Status Summary
**Last Updated:** 2025-08-27  
**Current Hypothesis:** Network/gateway related issue - tests pass locally but fail with remote gateways  
**Confidence Level:** High (based on local vs remote test results)

---

## Active Hypotheses

### H1: Remote Gateway Connection Issues ⚠️ **PRIMARY**
**Evidence For:**
- ✅ Tests pass with local gateway setup (3 local peers)
- ❌ Same tests fail with remote gateway
- Multiple UDP issues reported with AWS
- Port 31337 potentially problematic (known backdoor port)

**Evidence Against:**
- Issue persists across different cloud providers
- Issue persists with different ports

**Next Steps:**
- [ ] Confirm local success is reproducible
- [ ] Test with peers on different machines but same network
- [ ] Test with peers on completely different networks

### H2: Subscribe Flag with GET Requests
**Evidence For:**
- Subscribe=true not properly supported with GET requests
- May introduce race conditions

**Evidence Against:**
- Issue persists after removing subscribe flag from GET requests

**Status:** ✅ MITIGATED - Now using separate subscribe messages

### H3: Multiple River Users on Same Peer
**Evidence For:**
- Not well tested scenario
- May cause websocket API issues

**Evidence Against:**
- Not yet tested in isolation

**Next Steps:**
- [ ] Test with one user per peer exclusively

---

## Eliminated Hypotheses

### ~~Network Stability Issues~~
**Eliminated Because:**
- Ping app works consistently
- Connections stable when not using River
- Direct contract subscriptions work

---

## Test Results Log

### 2025-08-27: Local vs Remote Gateway Test
```
Test: Multi-user River end-to-end
Local Gateway (3 local peers): ✅ PASS
Remote Gateway (AWS): ❌ FAIL - Subscription timeout
Remote Gateway (Other provider): ❌ FAIL - Similar issues
```

### Variables to Test
- [ ] Peer isolation (different processes vs different machines vs different networks)
- [ ] Gateway location (local vs remote)
- [ ] Cloud provider (AWS vs others)
- [ ] Port numbers
- [ ] Single vs multiple subscriptions per peer

---

## Known Workarounds

1. **UDP/AWS Issues:** Use non-AWS provider or different port
2. **Subscribe with GET:** Use separate subscribe message after GET/PUT
3. **Multiple users per peer:** Use one peer per user

---

## Code Areas of Interest

- WebSocket API client-peer communication setup
- Contract initial setup flow (PUT → invite → join)
- Subscription handling with contracts
- Gateway-peer connection establishment

---

## Integration Test Coverage

### Working ✅
- Ping app end-to-end
- River with pre-existing rooms (direct subscription)
- Basic PUT/GET/SUBSCRIBE operations

### Failing ❌
- River room creation → invite → join flow (remote gateway)
- Multi-user scenarios on same peer

### To Be Tested
- [ ] River flow with peers on different physical machines
- [ ] River flow with peers on different networks
- [ ] Latency impact on operations

---

## Action Items

**Ian:**
- [ ] Document test configurations and results systematically
- [ ] Confirm local success is reproducible
- [ ] Provide SSH access to new machine for team

**Nacho/Hector:**
- [ ] Port test logic to integration test suite
- [ ] Reproduce issues locally if possible
- [ ] Review River PR integration tests

---

## Debugging Commands & Tools

```bash
# Local test setup (working)
# Start 3 local peers, one as gateway
python gateway_test_framework.py --local

# Remote test (failing)
python gateway_test_framework.py --version v0.1.19 --remote-gateway
```

---

## Meeting Notes

### 2025-08-27 Ian/Nacho/Hector
- Agreed to focus on systematic isolation of variables
- Will create integration tests similar to ping app
- Focus on initial room setup flow (create → invite → join)
- Avoid multiple subscriptions per peer for now