#!/usr/bin/env bash
#
# freenet-node -- the supervised, self-updating Freenet node, for Nix.
#
# WHY THIS EXISTS, AND WHY IT DOES NOT RESOLVE RELEASES ITSELF
# ------------------------------------------------------------
# `freenet update` replaces the RUNNING binary in place: it resolves
# `std::env::current_exe()` and renames a freshly-downloaded file over it
# (`replace_binary`, crates/core/src/bin/commands/update.rs), after verifying
# `SHA256SUMS.txt` against the ed25519 `FREENET_RELEASE_PUBKEY` baked into the
# binary. `/nix/store` is read-only, so that proven updater cannot run from a
# store path.
#
# So nix does not own the running binary. It SEEDS one into a mutable state
# directory, once, and from then on the node self-updates from there exactly as
# it does on every other platform -- same signature verification, same rollback
# snapshot, same crash probation, same known-bad pinning.
#
# THE LOAD-BEARING PROPERTY IS THAT BOTH `freenet network` AND `freenet update`
# RUN FROM THE STATE DIRECTORY, NEVER FROM THE STORE PATH. Run either from
# /nix/store and `current_exe()` resolves into a read-only filesystem, the
# updater's `fs::rename` fails with EROFS, and after MAX_UPDATE_FAILURES (3) the
# node stops asking to be updated at all -- a peer that is silently, permanently
# stale, which is the exact outcome this whole design exists to prevent.
# `scripts/nix-node-wrapper_test.sh` asserts WHICH binary each invocation ran.
#
# This script is a faithful port of the systemd unit the node generates for
# itself (`generate_user_service_file`, crates/core/src/bin/commands/service/
# linux.rs). The load-bearing property of that unit is that **systemd makes no
# update decisions**: it does not resolve tags, poll GitHub, or count releases.
# It restarts the node and runs `freenet update`; the UPDATER decides
# everything. Port that faithfully and this file needs no HTTP client, no tag
# parsing, no jitter and no release-poll interval.
#
#   IF YOU FIND YOURSELF WRITING UPDATE LOGIC IN THIS FILE, STOP. It belongs in
#   the updater, where it is tested, signature-checked and shared with every
#   other platform.
#
# The systemd directives this mirrors, and where each lands below:
#
#   Environment=FREENET_SUPERVISED=1  -> `export` below; the node logs calmly on
#                                       the exit-42 path instead of erroring
#                                       that nothing will apply the update
#                                       (#4580, auto_update.rs).
#   Restart=always / RestartSec=10    -> the `while` loop + `restart_pause`.
#   RestartSteps / RestartMaxDelaySec -> `restart_delay`, the growing backoff.
#   SuccessExitStatus=42 43           -> 42 and 43 are not counted as failures.
#   RestartPreventExitStatus=43       -> exit 43 stands down instead of looping.
#   ExecStopPost=... update --quiet   -> `run_updater`, for every exit except
#                                       0 and 43.
#   StartLimitBurst / IntervalSec     -> `count_failure`.
#
# Deliberate divergences from that unit, each for a stated reason:
#
#   * `Restart=always` restarts even on a clean exit 0. This wrapper EXITS on 0,
#     matching the macOS launchd wrapper (`generate_wrapper_script`,
#     service/macos.rs, "Normal shutdown"). systemd can distinguish an operator
#     `systemctl stop` from the service exiting by itself; a foreground wrapper
#     cannot, so restarting on 0 would make the node impossible to stop.
#   * `FREENET_SYSTEMD_FAST_CRASH` is deliberately NOT set. The node emits the
#     distinct fast-crash exit code 45 only when it sees that marker, and 45
#     wants `SuccessExitStatus` / `StartLimitBurst` handling this wrapper does
#     not reproduce exactly. Without it the node keeps emitting the
#     self-healing exit 42, which this wrapper does handle (auto_update.rs,
#     `SYSTEMD_FAST_CRASH_ENV_VAR`).
#
# KNOWN DIVERGENCE -- the #3967 stale-orphan pre-flight is NOT ported.
#
#     The unit runs an `ExecStartPre` before every start that finds whoever
#     holds the port and kills it IF it is an init-adopted orphan (PPID==1)
#     running a DIFFERENT `Freenet version:` than the binary about to launch.
#     Without it, an orphaned `freenet network` on an OLD binary can hold the
#     port forever: the node exits 43 on every start, this wrapper stands down,
#     and the orphan keeps serving stale assets with nothing to dislodge it.
#     That is a real, unattended-recovery gap and it is recorded here rather
#     than papered over -- an earlier version of this file asserted the port
#     holder "is healthy", which is precisely the case #3967 says it is not.
#
#     Not ported because the pre-flight is process-killing logic (`pgrep` over
#     every process of the user, /proc PPID parsing, TERM-then-KILL) whose
#     blast radius is another running node, and a faithful port needs a test
#     rig that fakes `pgrep` rather than one that may match a real peer on the
#     developer's machine. Two things bound the exposure in the meantime:
#     systemd's default `KillMode=control-group` already kills an orphan left
#     inside the unit's cgroup on every restart, which is where a wrapper-
#     spawned orphan lands; and `docs/nix.md` now specifies `Restart = "always"`
#     so a stood-down wrapper is retried rather than left dead forever. The
#     residual is an orphan OUTSIDE the cgroup (e.g. a hand-run `nix run` from
#     an earlier session). Port it, with a faked-`pgrep` test, if that shows up
#     in the field.
set -euo pipefail

# Store path of the nix-built binary used to SEED the mutable state directory.
# `nix/node.nix` assigns it immediately above this file's contents. The default
# keeps the file runnable -- and `shellcheck -x` clean -- on its own, which is
# what `scripts/nix-node-wrapper_test.sh` relies on.
: "${FREENET_NIX_SEED_BINARY:=}"

# RestartSec, and the RestartMaxDelaySec the backoff grows toward. Overridable
# so the wrapper's own test does not sleep for real; operators have no reason to
# change them.
RESTART_SECS="${FREENET_NODE_RESTART_SECS:-10}"
RESTART_MAX_SECS="${FREENET_NODE_RESTART_MAX_SECS:-300}"

# Upper bound on a single `freenet update` run. The unit gets this for free from
# TimeoutStopSec; a foreground wrapper does not, and a hung updater there blocks
# the supervisor with NO node running at all.
UPDATER_TIMEOUT_SECS="${FREENET_NODE_UPDATER_TIMEOUT_SECS:-600}"

# StartLimitBurst / StartLimitIntervalSec (#4551): more than 5 counted failures
# inside 120s stops the wrapper, rather than restart-looping forever. As in the
# unit, StartLimitAction is "stop loudly", not "reboot the host".
START_LIMIT_BURST="${FREENET_NODE_START_LIMIT_BURST:-5}"
START_LIMIT_INTERVAL_SECS="${FREENET_NODE_START_LIMIT_INTERVAL_SECS:-120}"

# ---------------------------------------------------------------------------
# Locate (and, on first run only, seed) the mutable binary.
# ---------------------------------------------------------------------------
state_dir="${STATE_DIRECTORY:-}"
# systemd passes StateDirectory= as a colon-separated LIST; take the first.
state_dir="${state_dir%%:*}"
if [ -n "$state_dir" ]; then
    : # systemd already told us where to put state.
elif [ -n "${XDG_STATE_HOME:-}" ]; then
    state_dir="$XDG_STATE_HOME/freenet"
elif [ -n "${HOME:-}" ]; then
    state_dir="$HOME/.local/state/freenet"
else
    echo "freenet-node: neither STATE_DIRECTORY, XDG_STATE_HOME nor HOME is set, so there is nowhere to put a writable binary." >&2
    exit 78
fi

bin_dir="$state_dir/bin"
binary="$bin_dir/freenet"

# The `Freenet version: ` line of a binary, prefix stripped and the following
# `Build timestamp:` line dropped: `0.2.135 (abc1234)`, or
# `0.2.136 (bbbb222-dirty)` for a build made from a dirty tree (`run_node`,
# crates/core/src/bin/freenet.rs). Prints NOTHING unless the output is
# unambiguously that line, because every caller treats "no version line" as
# "do not act".
version_line() {
    local out rest
    out="$(timeout 10 "$1" --version 2>/dev/null || true)"
    case "$out" in
        *"Freenet version: "*) ;;
        *) return 0 ;;
    esac
    rest="${out#*"Freenet version: "}"
    printf '%s' "${rest%%$'\n'*}"
}

# `Freenet version: 0.2.135 (abc1234)` -> `0.2.135`. Prints NOTHING unless the
# output is unambiguously that line with a dotted numeric version, because every
# caller treats "no version" as "do not act".
binary_version() {
    local ver
    ver="$(version_line "$1")"
    ver="${ver%%[!0-9.]*}"
    case "$ver" in
        [0-9]*.[0-9]*.[0-9]*) printf '%s' "$ver" ;;
        *) ;;
    esac
}

# Whether a binary was built from a dirty tree. `GIT_DIRTY` is one of the three
# auto-update kill switches (`auto_update_is_disabled`, crates/core/src/bin/
# freenet.rs), so a dirty binary NEVER updates itself, and installing one over a
# peer that still can is a one-way trip: the node stops exiting 42, so nothing
# in this design moves it forward again.
#
# THE MARKER IS PRINTED ON THE COMMIT HASH, NOT THE VERSION --
# `0.2.136 (bbbb222-dirty)` -- so `binary_version` cannot see it: it truncates at
# the first character that is not [0-9.] and yields `0.2.136`, indistinguishable
# from a clean release build of the same version. Hence a separate test.
binary_is_dirty() {
    case "$(version_line "$1")" in
        *-dirty\)*) return 0 ;;
        *) return 1 ;;
    esac
}

# The node's own known-bad pin (`KNOWN_BAD_FILE`, crates/core/src/bin/commands/
# rollback.rs): a plain-text file naming the single version that crash-looped on
# THIS host and was rolled back. `is_version_pinned_bad` makes the updater refuse
# to INSTALL that version -- but nothing in the node refuses to RUN one already
# in place, and the re-seed below writes $binary directly, with no
# `capture_known_good` snapshot and no probation marker. Re-seeding a pinned-bad
# version would therefore crash-loop with rollback unable to fire, and the pin is
# per-host state that whoever advanced the flake cannot see. So consult it here,
# before installing anything.
#
# Two directories, because the node resolves this one from HOME
# (`auto_update::state_dir()` is `dirs::home_dir()/.local/state/freenet`) and NOT
# from $STATE_DIRECTORY: under this script's XDG fallback the two are the same
# path, and under a systemd unit with `StateDirectory=` they are not. Either
# pinning this version is a refusal.
version_is_pinned_bad() {
    local want="$1" dir pinned
    local dirs=("$state_dir")
    if [ -n "${HOME:-}" ]; then
        dirs+=("$HOME/.local/state/freenet")
    fi
    for dir in "${dirs[@]}"; do
        [ -f "$dir/known_bad_version" ] || continue
        pinned="$(tr -d '[:space:]' <"$dir/known_bad_version" 2>/dev/null || true)"
        if [ -n "$pinned" ] && [ "$pinned" = "$want" ]; then
            return 0
        fi
    done
    return 1
}

# install(1) writes the DESTINATION IN PLACE, so a kill, an OOM or a full disk
# part-way through leaves a truncated file at $binary -- and the old `[ ! -e ]`
# gate then considered the peer seeded forever, so it never started again and
# never re-seeded. Write to a temp name in the SAME directory and rename: within
# one directory rename(2) is atomic, so $binary is only ever absent or complete.
seed_binary() {
    local why="$1" tmp now stale age
    if [ -z "$FREENET_NIX_SEED_BINARY" ]; then
        echo "freenet-node: no usable binary at $binary and no seed binary configured (FREENET_NIX_SEED_BINARY is empty)." >&2
        exit 78
    fi
    mkdir -p "$bin_dir"
    tmp="$bin_dir/.freenet.seed.$$"
    rm -f "$tmp"
    # The temp name carries THIS process's pid, so the line above never touches
    # the one a PREVIOUS seed left behind when it died mid-copy (the OOM /
    # reboot / ENOSPC shape case 11 of the test suite models). Those accumulate
    # one per killed start, forever, and a killed `install` can have written
    # most of a release binary before dying.
    #
    # AGE-BOUNDED, not a bare `rm .freenet.seed.*`: two wrappers can legitimately
    # start at once (that is what exit 43 exists for) and both seed before
    # either starts a node, so a blanket sweep would delete a SIBLING'S temp
    # mid-copy and fail its `mv`. A live seed is seconds old; an hour is far
    # past any of them and unambiguously wreckage. `stat` rather than `find`:
    # coreutils is the only runtime input nix/node.nix declares.
    now="$(date +%s)"
    for stale in "$bin_dir"/.freenet.seed.*; do
        # An unmatched glob stays literal, so test for existence first.
        if [ ! -e "$stale" ] || [ "$stale" = "$tmp" ]; then
            continue
        fi
        age="$((now - $(stat -c '%Y' "$stale" 2>/dev/null || printf '%s' "$now")))"
        if [ "$age" -gt 3600 ]; then
            rm -f "$stale"
        fi
    done
    install -m 0755 "$FREENET_NIX_SEED_BINARY" "$tmp"
    mv -f "$tmp" "$binary"
    echo "freenet-node: seeded $binary from $FREENET_NIX_SEED_BINARY ($why; the node owns it from now on and will update it in place)."
    # ...except when it cannot. A dirty build never auto-updates, so the line
    # above would be a promise this binary is unable to keep; say so rather than
    # leave an operator believing the peer is self-maintaining when it is frozen.
    # Only reachable on a FIRST seed (or a repair) -- the re-seed path below
    # refuses a dirty store binary outright, because there it would be replacing
    # a peer that still updates itself.
    if binary_is_dirty "$binary"; then
        echo "freenet-node: WARNING -- $binary is a -dirty build, which never auto-updates (GIT_DIRTY is an auto-update kill switch). This peer will NOT keep itself current; seed it from a clean release build before leaving it running." >&2
    fi
}

# Copied, never symlinked: the node must be able to rename a new file over this
# path, and it must survive `nix-collect-garbage` removing the store path this
# generation was built from. A symlink into /nix/store makes `current_exe()`
# resolve read-only and every update fail with EROFS.
if [ ! -e "$binary" ] && [ ! -L "$binary" ]; then
    seed_binary "first run"
elif [ -L "$binary" ]; then
    # Never produced by this script, but a hand-placed symlink is the exact
    # shape that silently disables updating, so replace it rather than run it.
    seed_binary "replacing a symlink, which the in-place updater cannot rename over"
elif [ ! -f "$binary" ]; then
    echo "freenet-node: $binary exists but is not a regular file; refusing to touch it." >&2
    exit 78
elif [ ! -x "$binary" ]; then
    # `replace_binary` chmods its temp file to 0755 BEFORE renaming it into
    # place, so a legitimately-updated binary is always executable. A
    # non-executable one is wreckage from an interrupted seed by an older
    # version of this script, which had no atomic write.
    seed_binary "replacing a partially-written binary"
else
    # NEVER overwrite an existing binary from the store merely because it
    # differs: after the first update the state-dir binary is a NEWER release
    # than this derivation was built from, and re-seeding on every start would
    # silently pin the node to the flake's version -- the whole failure this
    # design exists to avoid.
    #
    # The ONE exception is a store binary that is STRICTLY NEWER, which is the
    # only unattended escape from a state binary that can no longer update
    # itself: a `-dirty` build (FREENET_GIT_IS_DIRTY=1, or `nix run .` in a
    # dirty checkout) never auto-updates at all, and a binary that has hit the
    # MAX_UPDATE_FAILURES lockout has stopped trying. Either way the node never
    # exits 42 again, so nothing else in this design can ever move it forward.
    # Stepping FORWARD onto a newer store binary preserves "never pin
    # backwards"; both versions must parse or nothing happens.
    #
    # ...and "newer" is necessary but NOT sufficient. This exception exists to
    # move a peer that cannot update itself onto one that can, so it must refuse
    # any candidate that would land the peer in the same hole or a deeper one:
    # a `-dirty` store binary (never auto-updates, so the re-seed would be the
    # LAST move this peer ever makes) and a version this node has pinned
    # known-bad (crash-loops, and the re-seed installs it with no probation
    # marker, so auto-rollback cannot fire). See the two guards below.
    seed_version="$(binary_version "${FREENET_NIX_SEED_BINARY:-/nonexistent}")"
    state_version="$(binary_version "$binary")"
    if [ -n "$seed_version" ] && [ -n "$state_version" ] && [ "$seed_version" != "$state_version" ]; then
        newest="$(printf '%s\n%s\n' "$state_version" "$seed_version" | sort -V)"
        newest="${newest##*$'\n'}"
        if [ "$newest" = "$seed_version" ]; then
            # Two refusals. Both would otherwise replace a peer that CAN still
            # update itself with one that cannot -- the exact failure this whole
            # design exists to prevent, reached through the escape hatch that
            # exists to prevent it. A NEWER version number is not on its own
            # evidence that stepping onto it is forward progress.
            if binary_is_dirty "${FREENET_NIX_SEED_BINARY:-/nonexistent}"; then
                echo "freenet-node: the store binary $seed_version is newer than the state binary $state_version, but it is a -dirty build, which never auto-updates. NOT re-seeding: $binary stays in place and keeps updating itself. Build from a clean tree if you meant to move this peer forward." >&2
            elif version_is_pinned_bad "$seed_version"; then
                echo "freenet-node: the store binary $seed_version is newer than the state binary $state_version, but this node pinned $seed_version KNOWN-BAD after it crash-looped here and was rolled back. NOT re-seeding: installing it again would crash-loop with no probation marker, so rollback could never fire. Advance the flake past $seed_version." >&2
            else
                seed_binary "the store binary $seed_version is newer than the state binary $state_version"
            fi
        fi
    fi
fi

# ---------------------------------------------------------------------------
# Supervise.
# ---------------------------------------------------------------------------

# Positive evidence for the node that SOMETHING will catch exit 42 and run
# `freenet update` before restarting it (#4580). Set only because the loop below
# genuinely honours that contract.
export FREENET_SUPERVISED=1

child=0

terminate() {
    trap - TERM INT HUP
    if [ "$child" -ne 0 ]; then
        kill -TERM "$child" 2>/dev/null || true
        wait "$child" 2>/dev/null || true
    fi
    exit 0
}
trap terminate TERM INT HUP

# Every blocking wait goes through here, backgrounded and `wait`ed rather than
# run in the foreground: bash defers a trap until a foreground command finishes,
# so a SIGTERM during a 300s restart pause or a stuck update would otherwise sit
# unhandled for the whole of it. Recording the pid in `child` also lets
# `terminate` tear the child down instead of orphaning it.
run_interruptible() {
    "$@" &
    child=$!
    wait "$child" || true
    child=0
}

# ExecStopPost. Forwards the node's exit status so crash-loop auto-rollback
# (#4073) can tell a post-stop restart from a manual update and count crashes of
# a version still on probation. Never fatal: the unit's '-' prefix means the
# hook's own result never affects the restart.
run_updater() {
    run_interruptible env "FREENET_POST_STOP_EXIT_CODE=$1" \
        timeout "$UPDATER_TIMEOUT_SECS" "$binary" update --quiet
}

# Size, mtime and inode: enough to see `freenet update` rename a new file over
# the binary, which is what "the update made progress" means here.
binary_fingerprint() {
    stat -c '%s:%Y:%i' "$binary" 2>/dev/null || printf 'missing'
}

failures=()

count_failure() {
    local now kept=() t
    now="$(date +%s)"
    for t in ${failures[@]+"${failures[@]}"}; do
        if [ "$((now - t))" -lt "$START_LIMIT_INTERVAL_SECS" ]; then
            kept+=("$t")
        fi
    done
    kept+=("$now")
    failures=("${kept[@]}")
    if [ "${#failures[@]}" -gt "$START_LIMIT_BURST" ]; then
        echo "freenet-node: more than $START_LIMIT_BURST failed starts within ${START_LIMIT_INTERVAL_SECS}s. Stopping so this does not restart-loop forever; see the logs above for why the node is failing." >&2
        exit 1
    fi
}

# RestartSteps / RestartMaxDelaySec (#4073). The unit grows its restart delay
# from RestartSec to RestartMaxDelaySec so a residual crash/exit loop slows down
# instead of reconnecting to the gateways every 10s forever. A flat RestartSec
# here is NOT the "degrades like systemd < 254" behaviour it was once described
# as: NixOS has shipped systemd >= 254 since 23.11, so nix is the one platform
# where that degradation never happens in practice.
#
# It matters most for exit 42, which is burst-exempt (correctly -- an update
# chain must not take a peer offline) and is ALSO FATAL_LISTENER_EXIT_CODE
# (node/p2p_impl.rs). A node whose listener keeps failing exits 42 every boot
# and is never counted, so without a growing delay it hammers the gateways
# forever. `restart_attempt` is stepped back down whenever the node makes
# progress, so a genuine multi-release update chain still runs at full speed.
restart_attempt=0

restart_delay() {
    local base="$RESTART_SECS" doublings=0
    while [ "$doublings" -lt "$((restart_attempt - 1))" ] && [ "$base" -lt "$RESTART_MAX_SECS" ]; do
        base=$((base * 2))
        doublings=$((doublings + 1))
    done
    if [ "$base" -gt "$RESTART_MAX_SECS" ]; then
        base="$RESTART_MAX_SECS"
    fi
    # +-20% jitter, per .claude/rules/bug-prevention-patterns.md: without it a
    # fleet that took the same bad release restarts in lockstep and arrives at
    # the gateways as one thundering herd.
    printf '%s %s' "$base" "$((base * (80 + RANDOM % 41) / 100))"
}

restart_pause() {
    local base jittered
    read -r base jittered <<<"$(restart_delay)"
    echo "freenet-node: restarting in ${jittered}s (attempt $restart_attempt, base ${base}s)."
    if [ "$jittered" -gt 0 ]; then
        run_interruptible sleep "$jittered"
    fi
}

while true; do
    # Backgrounded and `wait`ed rather than run in the foreground, so a SIGTERM
    # aimed at this wrapper alone runs the trap immediately instead of being
    # deferred until the node happens to exit.
    #
    # `$binary` is the STATE-DIRECTORY binary, never $FREENET_NIX_SEED_BINARY.
    # See the header: running the store path makes every future update fail.
    node_started="$(date +%s)"
    "$binary" network "$@" &
    child=$!
    exit_code=0
    wait "$child" || exit_code=$?
    child=0
    node_ran="$(($(date +%s) - node_started))"

    case "$exit_code" in
        0)
            echo "freenet-node: the node shut down cleanly; stopping."
            exit 0
            ;;
        43)
            # RestartPreventExitStatus=43: another instance already holds the
            # port, so restarting immediately would just lose the same race.
            # Reported as success, as SuccessExitStatus=42 43 does, and as the
            # macOS wrapper does. NOTE this does NOT establish that the holder
            # is healthy -- see the #3967 KNOWN DIVERGENCE in the header, and
            # run this under `Restart = "always"` so a stale holder does not
            # leave the peer stood down forever.
            echo "freenet-node: another Freenet instance is already holding the port; stopping and leaving the retry to the service manager."
            exit 0
            ;;
        *)
            # Everything else -- panic (101), early-startup error (1), the
            # update-needed exit 42, and the signal-shaped statuses
            # (129/130/141/143) -- restarts and runs `freenet update`, which
            # either steps forward onto a release that fixes it or, for a
            # version still on probation, counts the crash and eventually rolls
            # back (#4073).
            #
            # A SIGNAL-SHAPED STATUS IS NOT A DELIBERATE STOP HERE (corrects an
            # earlier reading of #5227). The unit's #5227 guard skips the hook
            # only for `$SERVICE_RESULT` = "success" AND `$EXIT_CODE` =
            # "killed" -- that is, only when SYSTEMD ITSELF was asked to stop
            # the unit. This wrapper's equivalent of that knowledge is its own
            # trap: a signal aimed at the service reaches the wrapper too (a
            # `systemctl stop` signals the whole control group, Ctrl-C signals
            # the whole foreground process group), `terminate` runs, and the
            # wrapper exits 0 without ever reaching this `case`. So arriving
            # here means the NODE ALONE was signalled -- an operator or monitor
            # `pkill`ing `freenet network`, `systemctl kill --kill-who=main`, a
            # container runtime signalling the leaf -- which is exactly the
            # case systemd restarts. Standing the peer down for it left the
            # node dead with nothing to bring it back.
            if [ "$exit_code" = "42" ]; then
                # Not counted toward the crash-loop limit: an update is the
                # system working, not failing (SuccessExitStatus=42).
                echo "freenet-node: the node requested an update (exit 42); running 'freenet update'."
            else
                echo "freenet-node: the node exited $exit_code; running 'freenet update' to step forward or roll back." >&2
            fi

            fingerprint_before="$(binary_fingerprint)"
            run_updater "$exit_code"
            restart_attempt=$((restart_attempt + 1))
            if [ "$(binary_fingerprint)" != "$fingerprint_before" ]; then
                # The updater actually replaced the binary, so this restart is
                # progress rather than a repeat. Step the backoff back down so a
                # peer catching up over several releases is not slowed by it.
                restart_attempt=1
            elif [ "$node_ran" -ge "$START_LIMIT_INTERVAL_SECS" ]; then
                # The node ran for longer than the crash-loop window before
                # dying: not a tight loop, so do not inherit an old backoff.
                restart_attempt=1
            fi

            if [ "$exit_code" != "42" ]; then
                count_failure
            fi
            ;;
    esac

    restart_pause
done
