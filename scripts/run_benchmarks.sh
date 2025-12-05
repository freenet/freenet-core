#!/bin/bash
# run_benchmarks.sh - Run transport benchmarks with minimal noise
#
# Usage:
#   ./scripts/run_benchmarks.sh              # Run all levels
#   ./scripts/run_benchmarks.sh level0       # Run only Level 0 (pure logic)
#   ./scripts/run_benchmarks.sh level0 level1 # Run Level 0 and 1
#   ./scripts/run_benchmarks.sh --validate   # Validate environment first

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=============================================="
echo "Freenet Transport Benchmarks - Minimal Noise"
echo "=============================================="
echo ""

# =============================================================================
# Environment Validation
# =============================================================================

validate_environment() {
    echo "Validating benchmark environment..."
    local warnings=0

    # Check CPU governor
    if [ -f /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor ]; then
        governor=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)
        if [ "$governor" != "performance" ]; then
            echo -e "${YELLOW}WARNING: CPU governor is '$governor' (should be 'performance')${NC}"
            echo "         Fix with: echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor"
            ((warnings++))
        else
            echo -e "${GREEN}✓ CPU governor: performance${NC}"
        fi
    fi

    # Check turbo boost
    if [ -f /sys/devices/system/cpu/intel_pstate/no_turbo ]; then
        turbo=$(cat /sys/devices/system/cpu/intel_pstate/no_turbo)
        if [ "$turbo" = "0" ]; then
            echo -e "${YELLOW}WARNING: Turbo boost is enabled (adds variance)${NC}"
            echo "         Fix with: echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo"
            ((warnings++))
        else
            echo -e "${GREEN}✓ Turbo boost: disabled${NC}"
        fi
    fi

    # Check isolated CPUs
    if [ -f /sys/devices/system/cpu/isolated ]; then
        isolated=$(cat /sys/devices/system/cpu/isolated)
        if [ -z "$isolated" ]; then
            echo -e "${YELLOW}WARNING: No isolated CPUs (Level 2+ will have variance)${NC}"
            echo "         Fix with kernel parameter: isolcpus=2,3 nohz_full=2,3"
            ((warnings++))
        else
            echo -e "${GREEN}✓ Isolated CPUs: $isolated${NC}"
        fi
    fi

    # Check if running in VM
    if [ -f /sys/hypervisor/type ]; then
        hypervisor=$(cat /sys/hypervisor/type 2>/dev/null || echo "unknown")
        echo -e "${YELLOW}WARNING: Running in VM ($hypervisor) - results may vary${NC}"
        ((warnings++))
    elif [ -f /.dockerenv ]; then
        echo -e "${YELLOW}WARNING: Running in Docker container${NC}"
        ((warnings++))
    else
        echo -e "${GREEN}✓ Running on bare metal${NC}"
    fi

    # Check CPU frequency
    if [ -f /sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq ]; then
        cur_freq=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq)
        max_freq=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_max_freq 2>/dev/null || echo "$cur_freq")
        freq_ghz=$(echo "scale=2; $cur_freq / 1000000" | bc)
        echo -e "${GREEN}✓ CPU frequency: ${freq_ghz} GHz${NC}"

        # Check if at max
        if [ "$cur_freq" -lt $((max_freq * 95 / 100)) ]; then
            echo -e "${YELLOW}  (not at max frequency)${NC}"
            ((warnings++))
        fi
    fi

    echo ""
    if [ $warnings -gt 0 ]; then
        echo -e "${YELLOW}$warnings warning(s) found. Results may have higher variance.${NC}"
        echo -e "${YELLOW}For Level 0 benchmarks, these warnings can be ignored.${NC}"
    else
        echo -e "${GREEN}Environment looks good for benchmarking!${NC}"
    fi
    echo ""
}

# =============================================================================
# Benchmark Execution
# =============================================================================

run_benchmarks() {
    local levels="$@"

    # Build filter for criterion
    local filter=""
    if [ -n "$levels" ]; then
        for level in $levels; do
            if [ -n "$filter" ]; then
                filter="$filter|$level"
            else
                filter="$level"
            fi
        done
    fi

    echo "Building benchmarks (without tracing)..."
    echo ""

    # Key: disable default features to turn off tracing
    # Then re-enable only what we need (redb for storage, websocket for API)
    local features="redb,websocket"

    cd "$REPO_ROOT"

    if [ -n "$filter" ]; then
        echo "Running benchmarks matching: $filter"
        echo ""
        cargo bench \
            --bench transport_perf \
            --no-default-features \
            --features "$features" \
            -- "$filter"
    else
        echo "Running all benchmarks..."
        echo ""
        cargo bench \
            --bench transport_perf \
            --no-default-features \
            --features "$features"
    fi
}

# =============================================================================
# Main
# =============================================================================

# Parse arguments
VALIDATE=0
LEVELS=""

for arg in "$@"; do
    case $arg in
        --validate|-v)
            VALIDATE=1
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS] [LEVELS...]"
            echo ""
            echo "Options:"
            echo "  --validate, -v  Validate environment before running"
            echo "  --help, -h      Show this help"
            echo ""
            echo "Levels:"
            echo "  level0          Pure logic benchmarks (no I/O, deterministic)"
            echo "  level1          Mock I/O benchmarks (channels, no syscalls)"
            echo "  level2          Loopback benchmarks (real sockets, kernel involved)"
            echo "  level3          Stress tests (high load, environment sensitive)"
            echo ""
            echo "Experimental (hypothesis testing):"
            echo "  experimental_packet   Packet size vs throughput (same syscalls)"
            echo "  experimental_tokio    Tokio vs std vs crossbeam overhead"
            echo "  experimental_combined Full pipeline with varying packet sizes"
            echo ""
            echo "Examples:"
            echo "  $0                           # Run all levels"
            echo "  $0 level0                    # Run only Level 0"
            echo "  $0 --validate level0         # Validate env, then run Level 0"
            echo "  $0 level0 level1             # Run Level 0 and Level 1"
            echo "  $0 experimental_packet       # Run packet size experiments"
            echo "  $0 experimental_tokio        # Run Tokio overhead experiments"
            exit 0
            ;;
        level*|experimental*)
            LEVELS="$LEVELS $arg"
            ;;
        *)
            echo "Unknown argument: $arg"
            exit 1
            ;;
    esac
done

# Validate if requested
if [ $VALIDATE -eq 1 ]; then
    validate_environment
fi

# Run benchmarks
run_benchmarks $LEVELS

echo ""
echo "=============================================="
echo "Benchmark results saved to: target/criterion/"
echo "Open target/criterion/report/index.html for HTML report"
echo "=============================================="
