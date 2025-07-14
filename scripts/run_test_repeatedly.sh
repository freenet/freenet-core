#!/bin/bash

# Run the test multiple times and save logs
for i in {1..10}; do
    echo "Running test attempt $i..."
    
    # Run the test with debug logging
    RUST_LOG=debug cargo test --package freenet --test operations test_put_contract -- --nocapture > test_logs/run_$i.log 2>&1
    
    # Check if the test passed or failed
    if [ $? -eq 0 ]; then
        mv test_logs/run_$i.log test_logs/success_$i.log
        echo "Test $i: SUCCESS"
    else
        mv test_logs/run_$i.log test_logs/failure_$i.log
        echo "Test $i: FAILURE"
    fi
    
    # Small delay between runs
    sleep 2
done

# Count successes and failures
echo "Summary:"
echo "Successes: $(ls test_logs/success_*.log 2>/dev/null | wc -l)"
echo "Failures: $(ls test_logs/failure_*.log 2>/dev/null | wc -l)"