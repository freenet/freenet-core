#!/bin/bash

# Test runner for detecting flakiness in test_multiple_clients_subscription
# Run at least 10 times and collect results

NUM_RUNS=${1:-10}
TEST_NAME="test_multiple_clients_subscription"
RESULTS_DIR="test_results_$(date +%Y%m%d_%H%M%S)"

mkdir -p "$RESULTS_DIR"

echo "Running $TEST_NAME $NUM_RUNS times..."
echo "Results will be saved to: $RESULTS_DIR"
echo "========================================"

passed=0
failed=0

for i in $(seq 1 $NUM_RUNS); do
    echo ""
    echo "Run $i/$NUM_RUNS - $(date +%H:%M:%S)"
    echo "----------------------------------------"

    # Run the test and capture output
    if cargo test --test operations "$TEST_NAME" -- --nocapture > "$RESULTS_DIR/run_${i}.log" 2>&1; then
        echo "✓ PASSED"
        ((passed++))
        echo "PASSED" > "$RESULTS_DIR/run_${i}.result"
    else
        echo "✗ FAILED"
        ((failed++))
        echo "FAILED" > "$RESULTS_DIR/run_${i}.result"

        # Extract failure details
        echo "Failure details:"
        tail -n 50 "$RESULTS_DIR/run_${i}.log" | grep -A 10 -E "(panicked|FAILED|Error|timeout)"
    fi
done

echo ""
echo "========================================"
echo "SUMMARY"
echo "========================================"
echo "Total runs: $NUM_RUNS"
echo "Passed: $passed"
echo "Failed: $failed"
echo "Success rate: $(awk "BEGIN {printf \"%.1f\", ($passed/$NUM_RUNS)*100}")%"
echo ""
echo "Detailed logs saved to: $RESULTS_DIR/"
