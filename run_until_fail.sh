#!/bin/bash
for i in {1..20}; do
    echo "=== Run $i ==="
    if ! RUST_LOG=freenet=debug,operations=debug cargo test test_multiple_clients_subscription -- --nocapture > test_run_$i.log 2>&1; then
        echo "Failed on run $i"
        echo "Log saved to test_run_$i.log"
        exit 1
    fi
    echo "Passed"
done
echo "All runs passed!"