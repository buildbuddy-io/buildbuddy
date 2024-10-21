// Fetch executions for an invocation, writing the results to stdout as a
// JSON-serialized GetExecutionResponse proto.
//
// Example usage: to replay all executions for an invocation, run:
//
// bazel run -- enterprise/tools/get_executions --api_key=$API_KEY --invocation_id=$IID \
//   | jq -r '"--execution_id=" + .execution[].executionId' \
//   | xargs bazel run -- enterprise/tools/replay_action ...

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/mattn/go-isatty"
	"google.golang.org/protobuf/encoding/protojson"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
)

var (
	bbURL        = flag.String("url", "https://app.buildbuddy.io", "BuildBuddy HTTP URL")
	apiKey       = flag.String("api_key", "", "BuildBuddy API key")
	invocationID = flag.String("invocation_id", "", "Invocation ID")
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	ctx := context.Background()
	b, err := protojson.Marshal(&espb.GetExecutionRequest{
		ExecutionLookup: &espb.ExecutionLookup{
			InvocationId: *invocationID,
		},
	})
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", *bbURL+"/rpc/BuildBuddyService/GetExecution", bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Add("Content-Type", "application/json")
	if *apiKey != "" {
		req.Header.Add("x-buildbuddy-api-key", *apiKey)
	}
	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer rsp.Body.Close()
	body, err := io.ReadAll(rsp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if rsp.StatusCode != 200 {
		return fmt.Errorf("HTTP %s: %q", rsp.Status, string(body))
	}
	if _, err := os.Stdout.Write(body); err != nil {
		return err
	}
	if isatty.IsTerminal(os.Stdout.Fd()) {
		if _, err := os.Stdout.Write([]byte{'\n'}); err != nil {
			return err
		}
	}
	return nil
}
