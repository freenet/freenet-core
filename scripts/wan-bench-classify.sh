#!/usr/bin/env bash
# Decides whether a WAN Bench R1 known-failure run (BBR #5929, LEDBAT++ #5930)
# failed the known way. Used by the "known failures" step of
# .github/workflows/simulation-nightly.yml; tested by
# scripts/wan-bench-classify_test.sh.
#
# Usage: wan-bench-classify.sh <log> <max-abort-seeds | all>
#
# The log is the test's output. Its verdict lists one line per failed check,
# `  seed 0x<hex>: <reason>` (r1_tests.rs `gate`, including a seed whose run
# panicked). Judged per seed, not per line:
#   - "did not complete within" and "goodput never reached" are the known
#     slowness and always allowed.
#   - "sender gave up on the stream" is the known cwnd-wait abort. At most
#     <max-abort-seeds> seeds may report it ("all" for no limit).
#   - "no evidence the spike drove" is allowed only on a seed that also
#     aborted: a stream that died before the spiked ACKs returned leaves no
#     evidence. On a seed that did not abort it means the controller stopped
#     reacting to the spike.
#   - Any other per-seed reason, a seed that panicked included, is a new
#     failure. `across seeds:` lines are not judged here.
#
# Exit 0 when the run failed only the known way. Exit 1 otherwise, printing
# one line per reason that is not covered.

set -euo pipefail

if [ "$#" -ne 2 ]; then
    echo "usage: $0 <log> <max-abort-seeds | all>" >&2
    exit 2
fi
log=$1
max_aborts=$2

if ! grep -qE '^R1b? failed for' "$log"; then
    echo "failed without reaching its verdict (panic, hang or build error)"
    exit 1
fi

awk -v max_aborts="$max_aborts" '
    /^  seed 0x[0-9a-f]+: / {
        seed = $2
        sub(/:$/, "", seed)
        reason = substr($0, index($0, ": ") + 2)
        if (reason ~ /^(did not complete within |goodput never reached )/) {
            next
        } else if (reason ~ /^sender gave up on the stream /) {
            aborted[seed] = 1
        } else if (reason ~ /^no evidence the spike drove/) {
            no_evidence[seed] = 1
        } else {
            uncovered[++n] = "seed " seed ": " reason
        }
    }
    END {
        for (seed in no_evidence) {
            if (!(seed in aborted)) {
                uncovered[++n] = "seed " seed ": no evidence the spike drove the sender, without an abort"
            }
        }
        aborts = 0
        for (seed in aborted) aborts++
        if (max_aborts != "all" && aborts > max_aborts + 0) {
            uncovered[++n] = aborts " seeds aborted the stream, at most " max_aborts " expected"
        }
        for (i = 1; i <= n; i++) print uncovered[i]
        exit (n > 0 ? 1 : 0)
    }
' "$log"
