#!/usr/bin/env bash
# Fail if a Linux x86_64 release binary contains C code built above baseline
# x86-64.
#
# 0.2.152 shipped with ring's and zstd's C code built for x86-64-v3, because the
# ubuntu-latest runner moved to Ubuntu 26.04, whose amd64v3 gcc emits AVX2 and
# BMI2 by default. Every CPU without AVX2 died with SIGILL on the node's first
# TLS handshake. `freenet --version` still worked on those CPUs, so running the
# binary briefly does not detect this.
#
# Checking the whole binary would not work: ring's assembly and some Rust code
# contain AVX behind runtime detection. The functions below are plain C with no
# such detection, so a forbidden instruction in them means the C compiler's
# baseline is wrong. This samples those functions: it does not see a crate that
# sets its own -march, or a Rust-level target-cpu change.
#
# The binary must keep its symbols, so enabling `strip` in the release profile
# fails this check. If a dependency update renames or removes a function, the
# check also fails: replace it in DEFAULT_SENTINELS with another plain C function
# found with `nm`.
#
# Usage: check-x86-64-baseline.sh BINARY...
# BASELINE_SENTINELS overrides the function list (space-separated ERE patterns,
# matched against the whole symbol name). Used by the test.
set -euo pipefail

DEFAULT_SENTINELS='ring_core_[0-9_]+__OPENSSL_cpuid_setup ring_core_[0-9_]+__x25519_ge_scalarmult_base HUF_readDTableX1_wksp'
read -r -a sentinels <<<"${BASELINE_SENTINELS:-$DEFAULT_SENTINELS}"

# Mnemonics an x86-64 baseline compiler never emits, from AVX and BMI down to
# SSE3. tzcnt is left out on purpose: it encodes as `rep bsf`, which gcc emits at
# baseline and which runs on every x86-64 CPU. pextrw, pinsrw and the pmin/pmax
# forms left out of the list are SSE or SSE2, so they are baseline.
AVX_BMI='v[a-z0-9]+|shlx|shrx|sarx|rorx|bzhi|bextr|andn|pext|pdep|mulx|blsr|blsi|blsmsk|lzcnt|adcx|adox|movbe'
SSE3_SSSE3='lddqu|movddup|movshdup|movsldup|haddp[sd]|hsubp[sd]|addsubp[sd]|pshufb|palignr|phadd[a-z]*|phsub[a-z]*|pmaddubsw|pmulhrsw|psign[bwd]|pabs[bwd]'
SSE4='popcnt|crc32[bwlq]?|pmulld|pmuldq|ptest|pblend[a-z]*|blendv?p[sd]|pmovsx[a-z]*|pmovzx[a-z]*|pmins[bd]|pminu[wd]|pmaxs[bd]|pmaxu[wd]|pextr[bdq]|pinsr[bdq]|round[sp][sd]|pcmpeqq|pcmpgtq|pcmp[ei]str[im]|packusdw|dpp[sd]|mpsadbw|phminposuw|insertps|extractps'
FORBIDDEN="^(${AVX_BMI}|${SSE3_SSSE3}|${SSE4})\$"

if [ "$#" -eq 0 ] || [ "${#sentinels[@]}" -eq 0 ]; then
    echo "usage: $0 BINARY... (with a non-empty BASELINE_SENTINELS, if set)" >&2
    exit 64
fi

failed=0
for binary in "$@"; do
    symbols="$(nm "$binary")"
    for sentinel in "${sentinels[@]}"; do
        # A missing symbol fails: otherwise a rename or a stripped binary would
        # turn this check into one that always passes.
        names="$(awk -v re="^(${sentinel})\$" '$3 ~ re { print $3 }' <<<"$symbols")"
        if [ -z "$names" ]; then
            echo "FAIL $binary: no symbol matches '$sentinel'. See the header of $0" >&2
            failed=1
            continue
        fi
        for name in $names; do
            disassembly="$(objdump -d --no-show-raw-insn --disassemble="$name" "$binary")"
            instructions="$(awk -F'\t' 'NF >= 2 { split($2, f, " "); print f[1] }' <<<"$disassembly")"
            if [ -z "$instructions" ]; then
                echo "FAIL $binary: no instructions disassembled for $name" >&2
                failed=1
                continue
            fi
            rc=0
            matches="$(grep -E "$FORBIDDEN" <<<"$instructions")" || rc=$?
            case "$rc" in
                0)
                    bad="$(sort -u <<<"$matches" | tr '\n' ' ')"
                    echo "FAIL $binary: $name uses instructions above baseline x86-64: $bad" >&2
                    failed=1
                    ;;
                1) echo "ok   $binary: $name" ;;
                *)
                    echo "FAIL $binary: grep failed (exit $rc) checking $name" >&2
                    failed=1
                    ;;
            esac
        done
    done
done

exit "$failed"
