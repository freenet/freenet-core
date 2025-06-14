# GitHub Issue Cleanup Summary

## Issues Already Closed (8 issues)
- #1658 - Keep-alive timer destroyed between recv() calls - **Fixed in PR #1660**
- #1656 - Channel overflow causing connection timeouts - **Fixed with send() change**
- #1654 - Race condition in connection manager - **Fixed by channel overflow fix**
- #1653 - Improve bandwidth limiting and flow control - **Addressed in PR #1652**
- #1646 - Implement Flow Control for Transport Layer - **Duplicate of #1653**
- #1582 - Peer getting spammed by unexpected packets - **Fixed by channel overflow fix**
- #1583 - Peer sometimes unable to establish any connections - **Fixed by transport fixes**
- #1559 - "fdev query" triggers Connection reset - **Fixed by transport fixes**

## Issues to Keep Open
### Active Development (2)
- #1659 - Add simple PUT retry logic - **Awaiting developer feedback**
- #1640 - Add integration tests to CI workflow - **Still needed**

### Test Issues That May Be Fixed (3)
- #1637 - Fix flaky test: test_multiple_clients_subscription - **Test is no longer ignored, monitoring**
- #1624 - test_ping_partially_connected_network fails on CI - **May be fixed by transport improvements**
- #1616 - Fix ignored ping tests with network topology - **May be fixed by transport improvements**

### Documentation/Feature Requests (5)
- #1634 - Automate release process with GitHub Actions
- #1602 - Add a README.md to freenet crate
- #1597 - Use existing Backoff utility in update.rs
- #1552 - Document freenet-scaffold crates
- #1548 - Publish Freenet versions with Github Actions

### User-Reported Issues (3)
- #1595 - Freenet node setup script issue (by shadow-glass)
- #1561 - Docker Image Premade (by Merith-TK)
- #1522 - Status of Docker Codebase? (by shadow-glass)

### Technical Debt (10)
- #1631 - Improve contract loading in ping app tests
- #1611 - Fix simulation infrastructure bitrot in fdev
- #1588 - Subscription and Update Operations Review
- #1577 - Potential Bug: Unsubscribed handler calls subscribe
- #1566 - test_ping_application_loop failures
- #1565 - Unit test failures for compile_webapp_contract
- #1540 - Panic: "not yet implemented: FIXME: delegate op"
- #1525 - Secret type should be wrapper struct
- #1524 - Rename SecretsId to SecretId
- #1523 - Create integration test for attested contract

### Old Architecture/Long-term (8)
- #1519 - HttpGateway::attested_contracts memory leak
- #1502, #1501, #1500, #1499, #1498 - Delegate-related improvements
- #1497 - Create Dioxus Freenet library
- #1491 - Improve client_api::Error enum
- #1490 - Add `freenet service enable/disable` subcommand

### Very Old Issues by Nacho (5)
- #1454 - Refactor core interfaces
- #1453 - WASM executor pool
- #1452 - Leverage streaming for data transfer
- #1451 - Add better support for delegates
- #1450 - Add support for update deltas

## Recommendation
We've closed 8 issues that were directly related to the recent transport layer problems. The remaining issues fall into categories:
1. **Active work** - Keep open (#1659, #1640)
2. **Test issues** - Monitor to see if fixed (#1637, #1624, #1616)
3. **Documentation/tooling** - Keep open but low priority
4. **User issues** - Need investigation (#1595, Docker-related)
5. **Technical debt** - Keep open, prioritize based on impact
6. **Long-term architecture** - Keep open but low priority

Total: Closed 8, keeping ~35 open (down from 43)