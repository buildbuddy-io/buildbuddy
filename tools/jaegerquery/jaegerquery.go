// CLI for jaeger FindTraces API, which supports basic aggregation/analysis of
// the fetched spans. Currently, it just prints average duration of the spans
// across all fetched traces.
//
// Basic usage: set up port-forwarding to jaeger on port 16685, then run:
//
//	$ bazel run -- tools/jaegerquery --service=app --tag=invocation_id=abc-123 --operation=build.bazel.remote.execution.v2.Execution/Execute
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"golang.org/x/exp/maps"

	jaegerpb "github.com/buildbuddy-io/buildbuddy/proto/jaeger"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
)

var (
	target        = flag.String("target", "grpc://localhost:16685", "jaeger.api_v2.QueryService gRPC target")
	serviceName   = flag.String("service", "", "Service name (value of --app.trace_service_name)")
	operationName = flag.String("operation", "", "Operation name (required)")
	tags          = flag.Slice("tag", []string{}, "Tag specified as a `NAME=VALUE pair`. Can be set more than once.")
	limit         = flag.Int("limit", 100, "Max number of operations to fetch")
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	flag.Parse()

	if err := os.Chdir(os.Getenv("BUILD_WORKSPACE_DIRECTORY")); err != nil {
		return err
	}

	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	tagsMap := map[string]string{}
	for _, t := range *tags {
		parts := strings.SplitN(t, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid tag %q (expected NAME=VALUE)", t)
		}
		tagsMap[parts[0]] = parts[1]
	}

	conn, err := grpc_client.DialSimple(*target)
	if err != nil {
		return err
	}
	defer conn.Close()

	jaeger := jaegerpb.NewQueryServiceClient(conn)
	stream, err := jaeger.FindTraces(ctx, &jaegerpb.FindTracesRequest{
		Query: &jaegerpb.TraceQueryParameters{
			ServiceName:   *serviceName,
			OperationName: *operationName,
			Tags:          tagsMap,
			StartTimeMax:  tspb.Now(),
			StartTimeMin:  tspb.New(time.Now().Add(-1 * time.Hour)),
			SearchDepth:   int32(*limit),
		},
	})
	if err != nil {
		return err
	}

	// Track unique trace IDs since this determines the number of results that
	// were actually fetched (which may be less than the limit).
	traceIDs := map[string]struct{}{}
	totalDurations := map[string]time.Duration{}
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		for _, span := range chunk.Spans {
			traceIDs[string(span.TraceId)] = struct{}{}
			totalDurations[span.OperationName] += span.Duration.AsDuration()
		}
	}

	// Sort by total duration, greatest to least
	operations := maps.Keys(totalDurations)
	sort.Slice(operations, func(i, j int) bool {
		return totalDurations[operations[i]] > totalDurations[operations[j]]
	})
	fmt.Printf("AVG_MS\tOP\n")
	for _, op := range operations {
		dur := totalDurations[op]
		fmt.Printf("%.02f\t%s\n", float64(dur.Milliseconds())/float64(len(traceIDs)), op)
	}

	return nil
}
