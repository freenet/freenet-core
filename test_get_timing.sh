#!/bin/bash
export CARGO_TARGET_DIR=~/cargo_target_dir
echo "Running GET timing test..."
cargo test test_small_network_get_failure --package freenet-ping-app --release -- --nocapture 2>&1 | \
  awk '/GET request sent/{print "GET sent at: " strftime("%H:%M:%S")} 
       /Get response after/{print "Response: " $0}
       /Second GET completed/{print "Second GET: " $0}'