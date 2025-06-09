#!/bin/bash

echo "Analyzing race condition in connection establishment..."

# Extract key timing information from both logs
echo -e "\n=== Success Case (test 11) ==="
echo "Connection attempts:"
grep -E "(Connecting to peer|connection established)" test_logs/info_success_11.log | head -10

echo -e "\nDecryption errors:"
grep "gateway connection error.*decryption" test_logs/info_success_11.log | wc -l

echo -e "\n=== Failure Case (test 12) ==="
echo "Connection attempts:" 
grep -E "(Connecting to peer|connection established)" test_logs/info_failure_12.log | head -10

echo -e "\nDecryption errors:"
grep "gateway connection error.*decryption" test_logs/info_failure_12.log | wc -l

echo -e "\nChannel closed errors:"
grep "failed notifying, channel closed" test_logs/info_failure_12.log

echo -e "\n=== Packet routing analysis ==="
echo "Checking if existing connections are being ignored..."

# Look for patterns that indicate misrouted packets
echo -e "\nPackets received for existing connections (success):"
grep "received packet from remote.*has_remote_conn: true" test_logs/info_success_11.log | wc -l

echo -e "\nPackets received for existing connections (failure):"
grep "received packet from remote.*has_remote_conn: true" test_logs/info_failure_12.log | wc -l