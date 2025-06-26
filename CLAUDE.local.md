- contract states are commutative monoids, they can be "merged" in any order to arrive at the same result. This may reduce some potential race conditions.

## 📁 Organized Notes System

All debugging notes have been reorganized into `claude.notes.local/` for better navigation:

### [→ Go to Main Index](./claude.notes.local/index.md)

### Quick Links
- **[Current Focus: Connection Stability](./claude.notes.local/active/connection-stability-2025-06-25.md)**
- **[Active Hypotheses](./claude.notes.local/active/hypotheses.md)**
- **[Essential Commands](./claude.notes.local/reference/commands.md)**
- **[Code Locations](./claude.notes.local/reference/code-locations.md)**

## Important Testing Notes

### Always Use Network Mode for Testing
- **NEVER use local mode for testing** - it uses very different code paths
- Local mode bypasses critical networking components that need to be tested
- Always test with `freenet network` to ensure realistic behavior

## Current Priority (2025-06-25)

Per discussion with Nacho:
1. **Focus**: Connection stability to gateways
2. **Goal**: 10+ minute stable connections
3. **Method**: Create minimal transport test binary
4. **Avoid**: Getting distracted by River/contract issues