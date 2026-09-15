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
# directory and from then on the node self-updates from there exactly as it does
# on every other platform -- same signature verification, same rollback
# snapshot, same crash probation, same known-bad pinning.
#
# THIS SCRIPT ASKS ONE QUESTION AND NOTHING ELSE: can the binary in the state
# directory update itself? If it can, the store copy is never written over it.
# If it cannot -- absent, a symlink, a partial copy, not a freenet binary, or a
# `-dirty` build that can never exit 42 -- the store copy replaces it. That
# predicate, not a version comparison, is the whole placement policy; the full
# reasoning is on the decision block below.
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
# Locate the mutable binary -- and, when it cannot update itself, replace it.
#
# THE INVARIANT IS "ALWAYS END UP ON A BINARY THAT CAN UPDATE ITSELF".
#
# It is NOT "never move backwards in version". That is what earlier revisions
# of this file tried to encode, as a growing pile of refusals, and each new
# refusal opened a new stuck corner -- because version ordering is the wrong
# question. A CLEAN OLDER binary is forward progress: it exits 42 on its next
# start and walks itself to the current release. A DIRTY NEWER binary is a dead
# end: GIT_DIRTY is one of the three auto-update kill switches
# (`auto_update_is_disabled`, crates/core/src/bin/freenet.rs), the node never
# exits 42 again, and nothing in this design moves it forward. Version ordering
# only discriminates between two binaries that can BOTH update themselves, and
# there it barely matters, because whichever one runs reaches the newest release
# on its own.
#
# So this file asks ONE question, of the binary in the state directory:
#
#     can it update itself?
#
#   yes -> leave it completely alone, whatever the store holds. This is the
#          common path, and it is the whole reason the flake's version never
#          pins a peer that has already moved past it.
#   no  -> replace it with the store binary, if that is an improvement.
#
# "An improvement" has two shapes, and neither of them is a version comparison:
#
#   * the state binary still RUNS but cannot update itself (a `-dirty` build).
#     Swap only for a store binary that can update itself: trading one frozen
#     binary for another frozen binary gains nothing and risks losing a peer
#     that at least serves the network.
#   * the state binary cannot even run -- absent, a symlink, non-executable, or
#     not a freenet binary at all. Anything runnable beats nothing, so the store
#     binary goes in even if it is itself frozen, with the every-start warning
#     below saying so.
#
# THE ONE REFUSAL THAT IS GENUINELY ABOUT WHICH VERSION is the node's own
# known-bad pin: do not install a version THIS host crash-looped on and rolled
# back from. It applies only in the first shape, where there is a running peer
# to keep instead. In the second there is nothing else to run, and a peer that
# flaps loudly -- and that this wrapper's own `freenet update` can step forward
# out of as soon as a newer release exists -- beats a peer that is simply down.
#
# WHAT A RE-SEED DOES NOT DO, stated here because it undercuts that refusal:
# `capture_known_good` and `begin_probation` run only inside `commands::update`,
# so EVERY binary this script installs -- not only a pinned-bad one -- arrives
# with no known-good snapshot and no probation marker. The #4073 crash-loop
# rollback therefore cannot fire for any version the wrapper itself first put
# there, which means the pin below stands in for a safety net that is not
# present rather than backing one up. It is not a WRONG rollback:
# `handle_post_stop_at` drops a probation marker left by a different version
# instead of mis-applying it. And the failure is loud -- `count_failure` below,
# plus `Restart=always` in the documented unit -- rather than silent.
#
# AND A CORRECTION TO AN EARLIER VERSION OF THIS COMMENT, which justified the
# old version-forward re-seed as the escape from a MAX_UPDATE_FAILURES lockout.
# It is not, twice over. The lockout is a COUNTER FILE (`update_failures`, in
# `auto_update::state_dir()`) -- host state, not binary state -- so installing a
# different binary does not clear it. And it does not gate this wrapper's update
# path at all: `commands::update::run` never consults `should_attempt_update()`,
# so the ExecStopPost `freenet update` below runs regardless and clears the
# counter on a successful install. What the lockout genuinely stops is the
# node's in-process exit-42 re-poll, so a locked-out peer that never crashes
# also never updates. That residual is real, and this script cannot close it
# without making update decisions, which is the one thing it must not do.
# ---------------------------------------------------------------------------

# `dirs::home_dir()` -- which is what `auto_update::state_dir()` is built from --
# does NOT give up when $HOME is unset or empty: `dirs-sys` falls back to
# `getpwuid_r(geteuid())`. So the NODE still resolves a state directory, and
# still writes its known-bad pin into one, in an environment where $HOME is
# absent. systemd exports $HOME only for a unit that sets `User=`, so a root
# unit without one, or any scrubbed container, is exactly that environment.
#
# Mirror the fallback rather than guarding on `[ -n "$HOME" ]`, which failed
# OPEN: with the same pin on disk and only $HOME differing, the lookup refused
# the pinned-bad version with $HOME set and installed it with $HOME unset,
# saying nothing at all about the lookup it had skipped.
passwd_home() {
    local uid line home="" uid_field home_field _name _pw _gid _gecos _shell
    uid="$(id -u 2>/dev/null || true)"
    [ -n "$uid" ] || return 1
    line="$(getent passwd "$uid" 2>/dev/null || true)"
    if [ -n "$line" ]; then
        home="$(printf '%s\n' "$line" | cut -d: -f6)"
    elif [ -r /etc/passwd ]; then
        # `getent` is glibc's, not coreutils', and nix/node.nix declares only
        # coreutils as a runtime input; a unit with a scrubbed PATH may not have
        # it. /etc/passwd is not authoritative under NSS, but it does carry
        # every statically-declared NixOS user, which is what docs/nix.md's
        # example unit uses.
        while IFS=: read -r _name _pw uid_field _gid _gecos home_field _shell; do
            if [ "$uid_field" = "$uid" ]; then
                home="$home_field"
                break
            fi
        done </etc/passwd
    fi
    [ -n "$home" ] || return 1
    printf '%s' "$home"
}

# The home directory the NODE will resolve, which is not necessarily $HOME.
node_home="${HOME:-}"
if [ -z "$node_home" ]; then
    node_home="$(passwd_home || true)"
fi

state_dir="${STATE_DIRECTORY:-}"
# systemd passes StateDirectory= as a colon-separated LIST; take the first.
state_dir="${state_dir%%:*}"
if [ -n "$state_dir" ]; then
    : # systemd already told us where to put state.
elif [ -n "${XDG_STATE_HOME:-}" ]; then
    state_dir="$XDG_STATE_HOME/freenet"
elif [ -n "$node_home" ]; then
    state_dir="$node_home/.local/state/freenet"
else
    echo "freenet-node: neither STATE_DIRECTORY, XDG_STATE_HOME nor a home directory (\$HOME, or this uid's passwd entry) is available, so there is nowhere to put a writable binary." >&2
    exit 78
fi

bin_dir="$state_dir/bin"
binary="$bin_dir/freenet"

# The `Freenet version: ` line of a binary, prefix stripped and the following
# `Build timestamp:` line dropped: `0.2.135 (abc1234)`, or
# `0.2.136 (bbbb222-dirty)` for a build made from a dirty tree (`run_node`,
# crates/core/src/bin/freenet.rs). Prints NOTHING unless the output is
# unambiguously that line, because every caller treats "no version line" as
# "this is not a usable freenet binary".
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
# output is unambiguously that line with a dotted numeric version. Used ONLY to
# ask the known-bad pin about a version -- nothing in this file orders two
# versions any more; see the invariant above.
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
# freenet.rs), so a dirty binary NEVER updates itself.
#
# THE MARKER IS PRINTED ON THE COMMIT HASH, NOT THE VERSION --
# `0.2.136 (bbbb222-dirty)` -- so `binary_version` cannot see it: it truncates at
# the first character that is not [0-9.] and yields `0.2.136`, indistinguishable
# from a clean release build of the same version. Hence a separate test.
#
# It doubles as "runnable but frozen": it can only be true of a binary that
# actually ran and printed a version line. So `binary_is_dirty` false PLUS a
# non-empty `self_update_blocker` means the binary cannot even run.
binary_is_dirty() {
    case "$(version_line "$1")" in
        *-dirty\)*) return 0 ;;
        *) return 1 ;;
    esac
}

# THE ONE QUESTION. Prints the reason this binary CANNOT update itself, and
# prints nothing at all if it can. Every caller reads "prints nothing" as
# "leave it alone".
#
# It runs `--version` twice (once here, once through `binary_is_dirty`). That is
# a couple of execs per wrapper start plus one per node start, which is not
# worth folding together at the price of a helper whose name stops saying what
# it tests.
self_update_blocker() {
    local path="$1"
    if [ -L "$path" ]; then
        printf '%s' "it is a symlink, and the in-place updater renames a new file over this PATH -- which replaces the link and leaves the node running whatever it pointed at, and into /nix/store it is worse still, because current_exe() is then read-only and every update fails with EROFS"
        return 0
    fi
    if [ ! -e "$path" ]; then
        printf '%s' "there is nothing at that path yet"
        return 0
    fi
    if [ ! -f "$path" ]; then
        printf '%s' "it is not a regular file"
        return 0
    fi
    if [ ! -x "$path" ]; then
        printf '%s' "it is not executable, so it is a partially-written binary from an interrupted copy -- replace_binary chmods its temp file 0755 BEFORE renaming it into place, so a genuinely updated binary is always executable"
        return 0
    fi
    if [ -z "$(version_line "$path")" ]; then
        printf '%s' "it does not print a 'Freenet version:' line, so it is not a usable freenet binary"
        return 0
    fi
    if binary_is_dirty "$path"; then
        printf '%s' "it is a -dirty build, which never auto-updates -- GIT_DIRTY is one of the three auto-update kill switches, so the node never exits 42 and nothing moves it forward"
        return 0
    fi
}

# The node's own known-bad pin (`KNOWN_BAD_FILE`, crates/core/src/bin/commands/
# rollback.rs): a plain-text file naming the single version that crash-looped on
# THIS host and was rolled back. `is_version_pinned_bad` makes the updater refuse
# to INSTALL that version -- but nothing in the node refuses to RUN one already
# in place, and this script writes $binary directly, with no `capture_known_good`
# snapshot and no probation marker. So consult it before replacing a peer that
# is still serving the network.
#
# Two directories, because the node resolves this one from its home directory
# (`auto_update::state_dir()`) and NOT from $STATE_DIRECTORY: under this script's
# XDG fallback the two are the same path, and under a systemd unit with
# `StateDirectory=` they are not. Either pinning this version is a refusal.
version_is_pinned_bad() {
    local want="$1" dir pinned
    local dirs=("$state_dir")
    if [ -n "$node_home" ]; then
        dirs+=("$node_home/.local/state/freenet")
    else
        # Fail LOUD rather than open. The node can still have written a pin of
        # its own (see `passwd_home`), and a silent skip reads exactly like "no
        # pin" -- which is the wrong direction to guess in.
        echo "freenet-node: WARNING -- no home directory could be resolved for this user, so the node's own copy of the known-bad pin could not be consulted; only $state_dir was checked." >&2
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

# Temp files left by a seed that died mid-copy. The name carries the DEAD
# process's pid, so nothing else ever removes them, and a killed `install` can
# have written most of a release binary first: one file per killed start,
# forever.
#
# Swept on EVERY start, not only on a start that seeds. A killed first seed is
# followed by a start that DOES seed, repairing the binary -- after which no
# later start seeds again, so a sweep living inside `seed_binary` never ran
# again and the wreckage stayed on disk for the life of the peer.
#
# AGE-BOUNDED, not a bare `rm .freenet.seed.*`: two wrappers can legitimately
# start at once (that is what exit 43 exists for) and both seed before either
# starts a node, so a blanket sweep would delete a SIBLING'S temp mid-copy and
# fail its `mv`. A live seed is seconds old; an hour is far past any of them and
# unambiguously wreckage. `stat` rather than `find`: coreutils is the only
# runtime input nix/node.nix declares.
sweep_stale_seed_temps() {
    local now stale age
    now="$(date +%s)"
    for stale in "$bin_dir"/.freenet.seed.*; do
        # An unmatched glob stays literal, so test for existence first.
        [ -e "$stale" ] || continue
        age="$((now - $(stat -c '%Y' "$stale" 2>/dev/null || printf '%s' "$now")))"
        if [ "$age" -gt 3600 ]; then
            rm -f "$stale"
        fi
    done
}

# install(1) writes the DESTINATION IN PLACE, so a kill, an OOM or a full disk
# part-way through leaves a truncated file at $binary -- and the old `[ ! -e ]`
# gate then considered the peer seeded forever, so it never started again and
# never re-seeded. Write to a temp name in the SAME directory and rename: within
# one directory rename(2) is atomic, so $binary is only ever absent or complete.
seed_binary() {
    local why="$1" tmp
    if [ -z "$FREENET_NIX_SEED_BINARY" ]; then
        echo "freenet-node: no usable binary at $binary and no seed binary configured (FREENET_NIX_SEED_BINARY is empty)." >&2
        exit 78
    fi
    mkdir -p "$bin_dir"
    # The temp name carries THIS process's pid, so it never collides with one a
    # PREVIOUS seed left behind when it died mid-copy; those are cleared by
    # `sweep_stale_seed_temps`, on every start.
    tmp="$bin_dir/.freenet.seed.$$"
    rm -f "$tmp"
    install -m 0755 "$FREENET_NIX_SEED_BINARY" "$tmp"
    mv -f "$tmp" "$binary"
    echo "freenet-node: seeded $binary from $FREENET_NIX_SEED_BINARY ($why)."
    if [ -z "$(self_update_blocker "$binary")" ]; then
        # Claim the update contract only when the binary can actually honour
        # it. The old wording asserted "the node owns it from now on and will
        # update it in place" unconditionally, so the one sentence an operator
        # would grep for to confirm a peer is self-maintaining was printed,
        # verbatim, on exactly the peers that were frozen.
        echo "freenet-node: the node owns $binary from now on and will update it in place."
    fi
    # The frozen case is NOT warned about here. It is warned about in the
    # supervise loop, on every node start -- a warning printed once at seed time
    # is invisible for the whole subsequent life of the peer it is about, which
    # is precisely the peer that needs looking at.
}

sweep_stale_seed_temps

# A directory, a fifo or a device at the binary path is not wreckage this script
# can have produced, and `mv` onto a directory moves the new binary INSIDE it
# rather than over it. Refuse, loudly, rather than guess.
if [ -e "$binary" ] && [ ! -L "$binary" ] && [ ! -f "$binary" ]; then
    echo "freenet-node: $binary exists but is not a regular file; refusing to touch it." >&2
    exit 78
fi

# Copied, never symlinked: the node must be able to rename a new file over this
# path, and it must survive `nix-collect-garbage` removing the store path this
# generation was built from.
state_blocker="$(self_update_blocker "$binary")"
if [ -n "$state_blocker" ]; then
    seed_blocker="$(self_update_blocker "${FREENET_NIX_SEED_BINARY:-/nonexistent}")"
    seed_version="$(binary_version "${FREENET_NIX_SEED_BINARY:-/nonexistent}")"
    if binary_is_dirty "$binary"; then
        # The state binary still SERVES the network -- it just cannot move
        # itself forward. Only a store binary that CAN is worth the swap.
        if [ -n "$seed_blocker" ]; then
            echo "freenet-node: $binary cannot update itself ($state_blocker), and neither can the store binary ($seed_blocker). NOT re-seeding: trading one frozen binary for another gains nothing. Build from a clean tree to move this peer forward." >&2
        elif [ -n "$seed_version" ] && version_is_pinned_bad "$seed_version"; then
            echo "freenet-node: $binary cannot update itself ($state_blocker), but the store binary $seed_version is pinned KNOWN-BAD on this host -- it crash-looped here and was rolled back. NOT re-seeding: this script installs a binary with no known-good snapshot and no probation marker, so #4073 rollback could not fire the second time. Advance the flake past $seed_version." >&2
        else
            seed_binary "the state binary cannot update itself: $state_blocker"
        fi
    else
        # The state binary cannot even RUN, so there is no peer to protect and
        # anything runnable is an improvement. This installs the store binary
        # even when it is itself frozen or pinned known-bad -- with a warning
        # naming what it got, rather than a refusal that leaves the peer down.
        if [ -n "$seed_version" ] && version_is_pinned_bad "$seed_version"; then
            echo "freenet-node: WARNING -- the store binary $seed_version is pinned KNOWN-BAD on this host, but there is nothing usable at $binary ($state_blocker), so it is installed anyway: a peer that flaps loudly -- and that this wrapper's own 'freenet update' can step forward out of on the next release -- beats a peer that is simply down. Advance the flake past $seed_version." >&2
        fi
        seed_binary "$state_blocker"
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
    # ON EVERY NODE START, not once at seed time. A peer whose binary cannot
    # update itself is frozen for as long as it runs, and the wrapper's start-up
    # decision block runs ONCE per wrapper start -- so a warning printed there
    # is emitted on the first start of a peer that is then silent about it
    # forever, which is exactly backwards. Re-asked each time because the binary
    # changes underneath this loop: `freenet update` renames a new one over it.
    running_blocker="$(self_update_blocker "$binary")"
    if [ -n "$running_blocker" ]; then
        echo "freenet-node: WARNING -- $binary cannot update itself ($running_blocker). This peer will NOT keep itself current, which makes it a liability for the network. Re-seed it from a clean release build, or remove $binary and restart so this wrapper replaces it." >&2
    fi

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
