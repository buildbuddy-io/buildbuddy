#!/usr/bin/env bash
set -eu

BB_GRPC=grpc://localhost:1985
BB_HTTP=http://localhost:8080

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

bazel build //tools/cas
cp ./bazel-bin/tools/cas/cas_/cas "$TMP/cas"
bazel build //tools/unmarshal
cp ./bazel-bin/tools/unmarshal/unmarshal_/unmarshal "$TMP/unmarshal"

IID=$(uuidgen)
bazel test //enterprise/server/test/manual/hog_cpu:hog_cpu_test \
  --invocation_id="$IID" \
  --runs_per_test="${N:-250}" \
  --nocache_test_results \
  --test_env=HOG_PERCENT="${HOG_PERCENT:-50}" \
  --test_output=errors \
  --notest_keep_going \
  --remote_executor="$BB_GRPC" \
  --bes_backend="$BB_GRPC" \
  --bes_results_url="$BB_HTTP/invocation/"

rpc() {
  curl -sSL --fail-with-body \
    -H 'Content-Type: application/json' \
    "$BB_HTTP/rpc/BuildBuddyService/$1" \
    --data "$2"
}

echo >"$TMP"/fetch_execute_response "#!/usr/bin/env bash
\"$TMP\"/cas --target=\"$BB_GRPC\" --digest=\$1 --type=ActionResult |
    jq -r '.stdoutRaw' |
    base64 --decode |
    \"$TMP/unmarshal\" --type=ExecuteResponse
    echo
"
chmod +x "$TMP"/fetch_execute_response

echo "Fetching execution results..."
rpc GetExecution '{"executionLookup": {"invocationId": "'"$IID"'"}}' |
  jq -r '
    .execution[]
    | select(.commandSnippet | startswith("test/test-setup.sh"))
    | .executeResponseDigest
    | (.hash + "/" + .sizeBytes)' |
  xargs -n1 echo "$TMP"/fetch_execute_response |
  parallel -j32 |
  grep -v null |
  python3 ./enterprise/server/test/manual/hog_cpu/execution_stats.py
