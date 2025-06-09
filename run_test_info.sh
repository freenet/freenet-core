#!/bin/bash

# Run the test multiple times with INFO logging
for i in {1..20}; do
    echo "Running test attempt $i..."
    
    # Run the test with info logging
    RUST_LOG=info cargo test --package freenet --test operations test_put_contract -- --nocapture > test_logs/info_run_$i.log 2>&1
    
    # Check if the test passed or failed
    if [ $? -eq 0 ]; then
        mv test_logs/info_run_$i.log test_logs/info_success_$i.log
        echo "Test $i: SUCCESS"
    else
        mv test_logs/info_run_$i.log test_logs/info_failure_$i.log
        echo "Test $i: FAILURE"
        # Stop on first failure to examine it
        break
    fi
done

# Count successes and failures
echo "Summary:"
echo "Successes: $(ls test_logs/info_success_*.log 2>/dev/null | wc -l)"
echo "Failures: $(ls test_logs/info_failure_*.log 2>/dev/null | wc -l)"