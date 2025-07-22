- contract states are commutative monoids, they can be "merged" in any order to arrive at the same result. This may reduce some potential race conditions.

## Important Testing Notes

### Always Use Network Mode for Testing
- **NEVER use local mode for testing** - it uses very different code paths
- Local mode bypasses critical networking components that need to be tested
- Always test with `freenet network` to ensure realistic behavior

## Quick Reference - Essential Commands

### River Development
```bash
# Publish River (use this, not custom scripts)
cd ~/code/freenet/river && RUST_MIN_STACK=16777216 cargo make publish-river-debug

# Verify River build time (CRITICAL - only way to confirm new version is served)
curl -s http://127.0.0.1:50509/v1/contract/web/BcfxyjCH4snaknrBoCiqhYc9UFvmiJvhsp5d4L5DuvRa/ | grep -o 'Built: [^<]*' | head -1
```

### Freenet Management
```bash
# Start Freenet
./target/release/freenet network > freenet-debug.log 2>&1 &

# Check status
ps aux | grep freenet | grep -v grep | grep -v tail | grep -v journalctl

# Monitor logs
tail -f freenet-debug.log
```

## Detailed Documentation Files

### Current Active Debugging
- **Directory**: `freenet-invitation-bug.local/` (consolidated debugging)
  - `README.md` - Overview and quick commands
  - `river-notes/` - River-specific debugging documentation
  - `contract-test/` - Minimal Rust test to reproduce PUT/GET issue
  
### River Invitation Bug (2025-01-18)
- **Status**: CONFIRMED - Contract operations hang on live network, work in integration tests
- **Root Cause**: Freenet node receives WebSocket requests but never responds
- **Test Directory**: `freenet-invitation-bug.local/live-network-test/`
- **Confirmed Findings**:
  - River correctly sends PUT/GET requests via WebSocket
  - Raw WebSocket test: Receives binary error response from server
  - freenet-stdlib test: GET request sent but never receives response (2min timeout)
  - Integration test `test_put_contract` passes when run in isolation
  - Issue affects both PUT and GET operations
- **Current Investigation**: Systematically debugging why Freenet node doesn't respond to contract operations
- **See**: `freenet-invitation-bug.local/river-notes/invitation-bug-analysis-update.md`

### Historical Analysis (Reference Only)
- **Transport Layer Issues**: See lines 3-145 in previous version of this file (archived)
- **River Testing Procedures**: See lines 97-145 in previous version of this file (archived)

### CI Tools
- **GitHub CI Monitoring**: `~/code/agent.scripts/wait-for-ci.sh [PR_NUMBER]`

### Testing Tools
- **Puppeteer Testing Guide**: `puppeteer-testing-guide.local.md` - Essential patterns for testing Dioxus apps with MCP Puppeteer tools
- **Playwright Notes**: `playwright-notes.local.md` - Learnings and patterns for testing River with Playwright MCP tools

## Key Code Locations
- **River Room Creation**: `/home/ian/code/freenet/river/ui/src/components/room_list/create_room_modal.rs`
- **River Room Synchronizer**: `/home/ian/code/freenet/river/ui/src/components/app/freenet_api/room_synchronizer.rs`
- **River Room Data**: `/home/ian/code/freenet/river/ui/src/room_data.rs`

## Organization Rules
1. **Check this file first** for command reference and active debugging directories
2. **Use standard commands** instead of creating custom scripts
3. **Verify River build timestamps** after publishing
4. **Create timestamped .local directories** for complex debugging sessions
5. **Update this index** when adding new debugging directories or tools