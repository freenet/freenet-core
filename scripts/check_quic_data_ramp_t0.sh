#!/usr/bin/env bash
# Check, when the QUIC data ramp release is cut, that its ramp start (T0) is
# close to now (#5940, #6138).
#
# T0 (`QUIC_DATA_RAMP_START` in crates/core/src/transport/quic/upgrade/policy.rs)
# is fixed in the binary. A publish long after T0 starts every node that updates
# part-way along the curve (24 h after T0, at 100%, skipping the ramp), and one
# long before T0 holds every node at 5% until T0. So the ramp release must be
# cut within WINDOW of T0, or T0 must be re-set in a new reviewed PR first.
#
# Only the release named by `QUIC_DATA_RAMP_RELEASE` (beside T0) is checked:
# T0 stays set after it (past 24 h it reads 100%, the steady state), and later
# releases must not be held to it. With `QUIC_DATA_RAMP_RELEASE = None`, or a
# release of another version, this prints a notice and exits 0.
#
# Usage:
#   check_quic_data_ramp_t0.sh --version X.Y.Z --live true|false
#                              [--now UNIX_SECS] [--policy PATH] [--window SECS]
#
# --now defaults to the clock (`date +%s`); tests pass it. A live run that
# fails the check exits 1. A dry run reports the same finding as a warning and
# exits 0, so a rehearsal shows it without stopping. A policy file the script
# cannot read (a constant missing, repeated or written another way) exits 2 in
# both modes: the check cannot say anything, so it does not pass.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POLICY="$SCRIPT_DIR/../crates/core/src/transport/quic/upgrade/policy.rs"
WINDOW=$((3 * 3600))
VERSION=""
LIVE=""
NOW=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --version) VERSION="${2:-}"; shift 2 ;;
        --live) LIVE="${2:-}"; shift 2 ;;
        --now) NOW="${2:-}"; shift 2 ;;
        --policy) POLICY="${2:-}"; shift 2 ;;
        --window) WINDOW="${2:-}"; shift 2 ;;
        *) echo "::error::check_quic_data_ramp_t0: unknown argument '$1'"; exit 2 ;;
    esac
done

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "::error::check_quic_data_ramp_t0: --version must be X.Y.Z (got '$VERSION')"
    exit 2
fi
if [[ "$LIVE" != "true" && "$LIVE" != "false" ]]; then
    echo "::error::check_quic_data_ramp_t0: --live must be true or false (got '$LIVE')"
    exit 2
fi
if [[ -z "$NOW" ]]; then
    NOW="$(date +%s)"
fi
if [[ ! "$NOW" =~ ^[0-9]+$ || ! "$WINDOW" =~ ^[0-9]+$ ]]; then
    echo "::error::check_quic_data_ramp_t0: --now and --window must be whole seconds"
    exit 2
fi
if [[ ! -f "$POLICY" ]]; then
    echo "::error::check_quic_data_ramp_t0: policy file not found: $POLICY"
    exit 2
fi

# Sets DECLARED to the value of the one declaration of constant $1 whose type
# is $2, or exits 2. The declaration starts a line and runs to its `;`,
# possibly over several lines (rustfmt wraps a long one), and its whitespace
# is collapsed, so `Some(\n    1_800_000_000,\n)` reads as `Some(1_800_000_000)`.
# Starting a line keeps a comment or test that mentions the name from being
# read; two declarations are refused rather than one picked. (Not a command
# substitution: its error message would be captured.)
DECLARED=""
read_declared() {
    local name="$1" type="$2" found count
    found="$(awk -v head="pub(in crate::transport) const ${name}: ${type} =" '
        index($0, head) == 1 { inside = 1; text = "" }
        inside { text = text " " $0; if ($0 ~ /;[[:space:]]*$/) { print text; inside = 0 } }
    ' "$POLICY")"
    count="$(printf '%s' "$found" | grep -c . || true)"
    if [[ "$count" != "1" ]]; then
        echo "::error::check_quic_data_ramp_t0: expected one declaration of ${name}: ${type} in $POLICY, found ${count}"
        exit 2
    fi
    found="${found#*= }"
    found="${found%;*}"
    # Collapse whitespace, then drop it inside the parentheses and a trailing
    # comma rustfmt adds to a wrapped argument.
    found="$(printf '%s' "$found" | tr -s '[:space:]' ' ')"
    found="${found// /}"
    found="${found/,)/)}"
    DECLARED="$found"
}

read_declared QUIC_DATA_RAMP_RELEASE 'Option<&str>'
release_value="$DECLARED"
read_declared QUIC_DATA_RAMP_START 'Option<u64>'
start_value="$DECLARED"

# Every finding below fails a live run and warns in a dry run.
finding() {
    if [[ "$LIVE" == "true" ]]; then
        echo "::error title=QUIC data ramp start::$1"
        exit 1
    fi
    echo "::warning title=QUIC data ramp start (dry run)::$1"
    exit 0
}

# T0 in seconds, or empty for None; a value written another way exits 2.
t0=""
if [[ "$start_value" != "None" ]]; then
    if [[ ! "$start_value" =~ ^Some\(([0-9_]+)\)$ ]]; then
        echo "::error::check_quic_data_ramp_t0: QUIC_DATA_RAMP_START must be None or Some(<unix seconds>) (got $start_value)"
        exit 2
    fi
    t0="${BASH_REMATCH[1]//_/}"
    if [[ ! "$t0" =~ ^[0-9]+$ ]]; then
        echo "::error::check_quic_data_ramp_t0: QUIC_DATA_RAMP_START has no digits (got $start_value)"
        exit 2
    fi
    t0=$((10#$t0))
fi

if [[ "$release_value" == "None" ]]; then
    if [[ -n "$t0" ]]; then
        # Fail closed: a ramp in the tree with no release named for it would
        # ship under whatever version is cut, unchecked.
        finding "QUIC_DATA_RAMP_START is set but QUIC_DATA_RAMP_RELEASE is None, so release $VERSION would ship the ramp unchecked. Name the ramp release beside T0 in a reviewed PR (docs/RELEASING.md)."
    fi
    echo "QUIC data ramp: no ramp release (QUIC_DATA_RAMP_RELEASE = None); nothing to check."
    exit 0
fi
if [[ ! "$release_value" =~ ^Some\(\"([0-9]+\.[0-9]+\.[0-9]+)\"\)$ ]]; then
    echo "::error::check_quic_data_ramp_t0: QUIC_DATA_RAMP_RELEASE must be None or Some(\"X.Y.Z\") (got $release_value)"
    exit 2
fi
ramp_release="${BASH_REMATCH[1]}"
if [[ "$ramp_release" != "$VERSION" ]]; then
    if [[ -z "$t0" ]]; then
        # The emergency stop: T0 set back to None, the release left named.
        echo "QUIC data ramp: no ramp start (QUIC_DATA_RAMP_START = None); release $VERSION is not checked."
        exit 0
    fi
    earlier="$(printf '%s\n%s\n' "$VERSION" "$ramp_release" | sort -V | head -1)"
    if [[ "$earlier" == "$VERSION" ]]; then
        # Fail closed: the ramp is in the tree and this version comes before
        # the one named for it, so it would ship the ramp under another
        # version (an auto-bump, or the name set ahead of the cut).
        finding "Release $VERSION comes before the QUIC data ramp release ($ramp_release) but carries the ramp (QUIC_DATA_RAMP_START is set), so it would ship the ramp unchecked. Cut the ramp release as $ramp_release, or change QUIC_DATA_RAMP_RELEASE in the same reviewed PR as T0 (docs/RELEASING.md)."
    fi
    if (( NOW + WINDOW < t0 )); then
        # A later version cut before T0 would start the ramp at 5% early.
        finding "Release $VERSION comes after the QUIC data ramp release ($ramp_release) but is cut more than $((WINDOW / 60)) min before T0 ($t0), so it would ship the ramp ahead of its start. Re-set T0 or the ramp release in a reviewed PR (docs/RELEASING.md)."
    fi
    echo "QUIC data ramp: release $VERSION comes after the ramp release ($ramp_release); not checked."
    exit 0
fi

# This is the ramp release.
if [[ -z "$t0" ]]; then
    finding "Release $VERSION is the QUIC data ramp release (QUIC_DATA_RAMP_RELEASE), but QUIC_DATA_RAMP_START is None. Set T0 to the planned publish time in a reviewed PR first (docs/RELEASING.md)."
fi
off=$((NOW - t0))
abs=${off#-}
if (( abs > WINDOW )); then
    if (( off > 0 )); then
        where="$((abs / 60)) min after"
    else
        where="$((abs / 60)) min before"
    fi
    finding "Release $VERSION is the QUIC data ramp release, and now ($NOW) is $where its ramp start T0 ($t0), past the $((WINDOW / 60)) min window. Re-set QUIC_DATA_RAMP_START to the planned publish time in a new reviewed PR before publishing (docs/RELEASING.md)."
fi
echo "QUIC data ramp: release $VERSION is the ramp release; now ($NOW) is within $((WINDOW / 60)) min of T0 ($t0), off by $((off / 60)) min."
exit 0
