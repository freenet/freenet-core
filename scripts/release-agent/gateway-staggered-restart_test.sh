#!/usr/bin/env bash
# Tests for scripts/gateway-staggered-restart.sh using a fake systemctl and a
# fake /proc. Run: bash scripts/release-agent/gateway-staggered-restart_test.sh
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="$HERE/../gateway-staggered-restart.sh"
T="$(mktemp -d)"
trap 'rm -rf "$T"' EXIT
FAILS=0

# Fake /proc: uptime of 100000s; pid 111 = companion; its exe is controlled.
mkdir -p "$T/proc/111"
echo "100000.00 1.00" > "$T/proc/uptime"
echo "binary" > "$T/freenet"

# Fake systemctl driven by files in $T/state: primary_since (monotonic us),
# companion_active, restart_fail, restart_log.
cat > "$T/systemctl" <<'EOF'
#!/usr/bin/env bash
S="$STATE"
case "$1" in
    show)
        unit="${5%.service}"; prop="$3"
        case "$unit:$prop" in
            freenet-gateway:ActiveState) cat "$S/primary_state" ;;
            freenet-gateway:ActiveEnterTimestampMonotonic) cat "$S/primary_since" ;;
            freenet-gateway-2:MainPID) cat "$S/mainpid" ;;
            freenet-gateway-2:NRestarts) cat "$S/nrestarts" ;;
        esac ;;
    restart)
        echo "$2" >> "$S/restart_log"
        [[ -e "$S/restart_fail" ]] && exit 1
        # a restart moves the companion onto the installed binary, unless the
        # test says the new process still shows the old (deleted) one
        [[ -e "$S/no_relink" ]] || ln -sfn "$INSTALL_PATH" "$PROC_ROOT/111/exe" ;;
    is-active)
        # simulate Restart=always replacing a crashed process during the settle wait
        if [[ -e "$S/crashloop" ]]; then echo 1 > "$S/nrestarts"; echo 222 > "$S/mainpid"; ln -sfn "$INSTALL_PATH" "$PROC_ROOT/222/exe"; fi
        cat "$S/companion_active" ;;
esac
EOF
chmod +x "$T/systemctl"

reset() {
    export STATE="$T/state"; rm -rf "$STATE"; mkdir -p "$STATE"
    echo active > "$STATE/primary_state"
    echo 90000000000 > "$STATE/primary_since"      # primary up 10000s
    echo active > "$STATE/companion_active"
    echo 111 > "$STATE/mainpid"; echo 0 > "$STATE/nrestarts"; mkdir -p "$T/proc/222"
    ln -sfn "$T/freenet (deleted)" "$T/proc/111/exe" # companion on the OLD binary
}
run() {
    INSTALL_PATH="$T/freenet" SYSTEMCTL="$T/systemctl" PROC_ROOT="$T/proc" \
        bash "$SCRIPT" --poll 1 --settle 0 "$@" 2>&1
}
check() { # name expected_exit actual_exit
    if [[ "$2" == "$3" ]]; then echo "ok   $1"; else echo "FAIL $1 (want exit $2, got $3)"; FAILS=$((FAILS+1)); fi
}
restarted() { [[ -s "$STATE/restart_log" ]]; }

reset; run --min-primary-uptime 600 --primary-timeout 5 >/dev/null; rc=$?
check "restarts the companion once the primary is old enough" 0 $rc
restarted && check "restart was issued" 0 0 || check "restart was issued" 0 1

reset; echo 99900000000 > "$STATE/primary_since"   # primary up 100s
run --min-primary-uptime 600 --primary-timeout 3 >/dev/null; rc=$?
check "fails visibly when the primary is too young" 1 $rc
restarted && check "no restart while the primary is young" 0 1 || check "no restart while the primary is young" 0 0

reset; echo inactive > "$STATE/primary_state"
run --min-primary-uptime 600 --primary-timeout 3 >/dev/null; rc=$?
check "fails visibly when the primary is down" 1 $rc

reset; touch "$STATE/restart_fail"
run --primary-timeout 3 >/dev/null; rc=$?
check "fails visibly when systemctl restart fails" 1 $rc

reset; ln -sfn "$T/freenet" "$T/proc/111/exe"
run --primary-timeout 3 >/dev/null; rc=$?
check "no-op when the companion already runs the installed binary" 0 $rc
restarted && check "no restart when already current" 0 1 || check "no restart when already current" 0 0

reset; echo failed > "$STATE/companion_active"
run --primary-timeout 3 >/dev/null; rc=$?
check "fails when the companion is not active after restart" 1 $rc

reset; touch "$STATE/crashloop"
run --primary-timeout 3 >/dev/null; rc=$?
check "fails when the companion crash-loops during the settle wait (new PID is active)" 1 $rc

reset; touch "$STATE/no_relink"
run --primary-timeout 3 >/dev/null; rc=$?
check "fails when the companion is active but still on the old binary" 1 $rc

# The already-current check must not wait for the primary at all.
reset; ln -sfn "$T/freenet" "$T/proc/111/exe"; echo inactive > "$STATE/primary_state"
run --primary-timeout 0 >/dev/null; rc=$?
check "current companion + down primary is still a no-op" 0 $rc

exit $((FAILS > 0))
