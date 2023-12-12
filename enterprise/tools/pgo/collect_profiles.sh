#!/usr/bin/env bash
# This script collects and merges prod CPU pprof files for pgo use.
#
# Example usage:
#   ./enterprise/tools/pgo/collect_profiles.sh
#
set -e

OUTPUT_DIR="$(dirname "$(readlink -f "$0")")"

pushd "$(mktemp -d -q)"
PROFILE_DIR="$(pwd)"

for i in {0..15}; do
    kubectl --namespace=buildbuddy-prod port-forward "buildbuddy-app-$i" 9090:9090 > /dev/null 2>&1 &
    pid=$!

    trap '{
      kill $pid > /dev/null 2>&1
    }' EXIT

    while ! nc -vz localhost 9090 > /dev/null 2>&1 ; do
      sleep 0.1
    done

    echo -n "Collecting data from app $i... "
    curl -sS "http://localhost:9090/debug/pprof/profile?seconds=30" > "app_${i}.cpu.pprof"
    echo "finished."

    kill "$pid"
    while nc -vz localhost 9090 > /dev/null 2>&1 ; do
      sleep 0.1
    done
done

popd

go tool pprof -proto ${PROFILE_DIR}/*.cpu.pprof > ${OUTPUT_DIR}/prod.pprof
