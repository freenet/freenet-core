#!/usr/bin/env bash
#
# CANARY_NODE_RUNNER for Windows (#5341).
#
# `auto-update-canary.sh` decides everything; this starts the process. See the
# "PLATFORM SEAM" block in `run_node_until_check` for the contract and for why
# the split is where it is. In one line: the assertions are the valuable part
# and there must be exactly one copy of them, so the only thing that gets a
# second implementation is the part that is genuinely different -- starting,
# watching and stopping a native Windows `.exe`.
#
# This file is the bash half of the runner and does two things:
#   1. converts the canary's POSIX paths to Windows paths (`cygpath`, which is
#      a bash-side tool, which is why this half exists at all rather than the
#      .ps1 being invoked directly);
#   2. invokes the .ps1 and passes its exit status straight through.
#
# It deliberately contains NO assertion, NO marker and NO verdict. If you find
# yourself adding one here, it belongs in auto-update-canary.sh instead --
# that is the file the fixture tests drive.
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PS1_PATH="$SCRIPT_DIR/canary-run-node-windows.ps1"

if [ "$#" -ne 2 ]; then
  echo "::error::usage: canary-run-node-windows.sh <binary> <workdir>" >&2
  exit 64
fi

binary="$1"
work="$2"

if [ ! -f "$PS1_PATH" ]; then
  echo "::error::$PS1_PATH is missing -- the Windows canary runner is only half present, so the gate cannot run. This is a harness failure, not an auto-update fault." >&2
  exit 1
fi

# `cygpath` ships with Git for Windows, which is what `shell: bash` runs on a
# windows-latest runner. Refusing loudly beats hand-rolling the conversion:
# MSYS's virtual root (`/tmp`, `/usr`) does not follow the obvious `/c/...`
# rule, and `mktemp -d` -- which is where CANARY_WORKDIR comes from -- returns
# exactly such a path.
if ! command -v cygpath >/dev/null 2>&1; then
  echo "::error::cygpath is not on PATH, so this runner cannot translate the canary's POSIX paths for a native Windows binary. Run this step with 'shell: bash' on a Windows runner (Git for Windows provides cygpath). Harness failure, not an auto-update fault." >&2
  exit 1
fi

win_binary="$(cygpath -w "$binary")" || {
  echo "::error::cygpath could not translate the binary path '$binary'." >&2
  exit 1
}
win_work="$(cygpath -w "$work")" || {
  echo "::error::cygpath could not translate the workdir path '$work'." >&2
  exit 1
}
win_ps1="$(cygpath -w "$PS1_PATH")" || {
  echo "::error::cygpath could not translate the runner path '$PS1_PATH'." >&2
  exit 1
}

# Windows PowerShell 5.1 is present on every windows-latest image and needs no
# install; pwsh is preferred when present only because it is the one a local
# developer is likelier to have. Either satisfies the script.
PS_EXE=""
for candidate in pwsh powershell.exe powershell; do
  if command -v "$candidate" >/dev/null 2>&1; then
    PS_EXE="$candidate"
    break
  fi
done
if [ -z "$PS_EXE" ]; then
  echo "::error::neither pwsh nor powershell is on PATH, so the Windows canary runner cannot start the node. Harness failure, not an auto-update fault." >&2
  exit 1
fi

echo "canary-run-node-windows: $PS_EXE -> $win_ps1"
echo "canary-run-node-windows: binary=$win_binary workdir=$win_work"

# No `|| true`, no status swallowing: the .ps1's exit status IS this runner's
# answer to "did the harness do its job", and the canary hard-blocks on a
# non-zero. Losing it here would turn a broken harness into a silent verdict.
"$PS_EXE" -NoProfile -NonInteractive -ExecutionPolicy Bypass \
  -File "$win_ps1" -Binary "$win_binary" -WorkDir "$win_work"
