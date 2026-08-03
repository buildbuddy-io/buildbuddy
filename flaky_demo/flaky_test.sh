#!/bin/bash
# Simulates a slow flaky test: burns time, then fails. With
# --flaky_test_attempts, Bazel retries it, producing retry_of chains in the
# execution graph log.
echo "doing some important test work..."
sleep 5
echo "flaked!"
exit 1
