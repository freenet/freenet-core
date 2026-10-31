#!/usr/bin/env bash
# Tests for check-x86-64-baseline.sh, using small binaries built with known flags.
set -uo pipefail

if [ "$(uname -m)" != "x86_64" ]; then
    echo "skipping: the fixtures need an x86_64 host"
    exit 0
fi

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
check="$here/check-x86-64-baseline.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
fails=0

cat >"$work/fixture.c" <<'EOF'
/* __builtin_popcount becomes popcnt at x86-64-v2 and above, and a libgcc call at
   baseline. The shift becomes shlx at x86-64-v3. */
__attribute__((noinline)) unsigned sentinel_fn(unsigned *a, unsigned n, unsigned s) {
    unsigned total = 0;
    for (unsigned i = 0; i < n; i++) total += (a[i] << s) + (unsigned)__builtin_popcount(a[i]);
    return total;
}
int main(int argc, char **argv) { return (int)sentinel_fn((unsigned *)argv, 0, (unsigned)argc); }
EOF

gcc -O3 -march=x86-64 -o "$work/baseline" "$work/fixture.c"
gcc -O3 -march=x86-64-v2 -o "$work/v2" "$work/fixture.c"
gcc -O3 -march=x86-64-v3 -o "$work/v3" "$work/fixture.c"
cp "$work/baseline" "$work/stripped" && strip "$work/stripped"

# expect EXIT_CODE OUTPUT_SUBSTRING DESCRIPTION -- BINARY
expect() {
    local want="$1" needle="$2" desc="$3" got output
    shift 3
    output="$(BASELINE_SENTINELS='sentinel_fn' bash "$check" "$@" 2>&1)"
    got=$?
    if [ "$got" -ne "$want" ] || [[ "$output" != *"$needle"* ]]; then
        echo "not ok - $desc (exit $got, wanted $want)"
        echo "$output"
        fails=1
    else
        echo "ok - $desc"
    fi
}

expect 0 'ok   ' 'a baseline build passes' "$work/baseline"
expect 1 'popcnt' 'an x86-64-v2 build fails' "$work/v2"
expect 1 'above baseline' 'an x86-64-v3 build fails' "$work/v3"
expect 1 'no symbol matches' 'a stripped binary fails rather than passing' "$work/stripped"
expect 1 'above baseline' 'one bad binary among several fails' "$work/baseline" "$work/v3"

# Build a binary whose sentinel_fn contains exactly one instruction. The function
# is never called, so it does not matter whether this host can run it.
build_with_instruction() {
    printf '__attribute__((used, noinline)) void sentinel_fn(void) { __asm__("%s"); }\nint main(void) { return 0; }\n' "$1" >"$work/one.c"
    gcc -O1 -o "$work/one" "$work/one.c"
}

# One instruction from each group in FORBIDDEN, so removing a group fails here.
# shellcheck disable=SC2016  # $1 is an assembler immediate, not a shell variable.
for insn in \
    'vmovd %eax,%xmm0' 'shlx %eax,%ecx,%edx' 'bextr %eax,%ecx,%edx' 'lzcnt %eax,%ecx' 'movbe (%rax),%ecx' \
    'haddpd %xmm1,%xmm0' 'lddqu (%rax),%xmm0' \
    'pshufb %xmm1,%xmm0' 'palignr $1,%xmm1,%xmm0' 'pabsw %xmm1,%xmm0' \
    'pmulld %xmm1,%xmm0' 'ptest %xmm1,%xmm0' 'pmaxud %xmm1,%xmm0' 'pextrd $1,%xmm0,%eax' \
    'crc32l %eax,%ecx' 'pcmpgtq %xmm1,%xmm0' 'popcnt %eax,%ecx'; do
    build_with_instruction "$insn"
    expect 1 'above baseline' "forbids: $insn" "$work/one"
done

# Baseline instructions with names close to forbidden ones must not be flagged.
# shellcheck disable=SC2016  # $1 is an assembler immediate, not a shell variable.
for insn in \
    'pextrw $1,%xmm0,%eax' 'pinsrw $1,%eax,%xmm0' 'pminub %xmm1,%xmm0' 'pminsw %xmm1,%xmm0' \
    'pmaxub %xmm1,%xmm0' 'pmaxsw %xmm1,%xmm0' 'pandn %xmm1,%xmm0' 'pshufd $1,%xmm1,%xmm0' \
    'bswap %eax' 'tzcnt %eax,%ecx'; do
    build_with_instruction "$insn"
    expect 0 'ok   ' "allows: $insn" "$work/one"
done

output="$(bash "$check" 2>&1)"
got=$?
if [ "$got" -eq 64 ]; then
    echo "ok - no arguments is a usage error"
else
    echo "not ok - no arguments exited $got: $output"
    fails=1
fi

output="$(BASELINE_SENTINELS='   ' bash "$check" "$work/baseline" 2>&1)"
got=$?
if [ "$got" -eq 64 ]; then
    echo "ok - a blank sentinel list is a usage error rather than a pass"
else
    echo "not ok - a blank sentinel list exited $got: $output"
    fails=1
fi

output="$(bash "$check" "$work/baseline" 2>&1)"
got=$?
if [ "$got" -eq 1 ] && [[ "$output" == *"no symbol matches"* ]]; then
    echo "ok - the default sentinels are required"
else
    echo "not ok - default sentinels missing from the fixture exited $got: $output"
    fails=1
fi

exit "$fails"
