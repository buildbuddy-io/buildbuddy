package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	batchSize       = 16
	maxJobs         = 100
	defaultPageSize = 1000
	digestFunction  = repb.DigestFunction_SHA256
	defaultTemplate = "{{.InvocationID}}:{{.LineNumber}}:{{.Match}}"
	invocationsEnv  = "BUILDGREP_INVOCATIONS"
)

var (
	target             = flag.String("target", "remote.buildbuddy.io", "BuildBuddy gRPC target.")
	remoteInstanceName = flag.String("remote_instance_name", "buildgrep", "Remote instance name to use for buildgrep actions.")
	apiKey             = flag.String("api_key", "", "BuildBuddy API key. Defaults to BUILDBUDDY_API_KEY.", flag.Secret)
	regex              = flag.Bool("re", false, "Treat the search pattern as a Go regular expression.")
	outputTemplate     = flag.String("template", defaultTemplate, "Go text/template for each matching line. Fields include InvocationID, UpdatedAt, UpdatedAtUsec, LineNumber, Match, Line, and MatchedText.")
	startTimeFlag      = flag.String("start", "", "Invocation history start time, as RFC3339 or unix seconds. Defaults to --stop minus --lookback.")
	stopTimeFlag       = flag.String("stop", "", "Invocation history stop time, as RFC3339 or unix seconds. Defaults to now.")
	lookback           = flag.Duration("lookback", 14*24*time.Hour, "How far back to search invocation history when --start is omitted.")
	pageSize           = flag.Int("page_size", defaultPageSize, "Number of invocations to fetch per SearchInvocation page.")
	jobs               = flag.Int("jobs", maxJobs, "Maximum number of concurrent remote grep actions. Values over 100 are capped.")
	remoteTimeout      = flag.Duration("remote_timeout", 30*time.Minute, "Timeout for each remote grep action.")
)

var (
	workerRlocationpath string
)

type workerConfig struct {
	Pattern  string `json:"pattern"`
	Regex    bool   `json:"regex"`
	Template string `json:"template"`
}

type batchInvocation struct {
	InvocationID  string `json:"invocation_id"`
	UpdatedAtUsec int64  `json:"updated_at_usec"`
}

type toolEnv struct {
	env    *real_environment.RealEnv
	conn   io.Closer
	client bbspb.BuildBuddyServiceClient
}

func main() {
	flag.Parse()
	if err := log.Configure(); err != nil {
		log.Fatalf("configure logging: %s", err)
	}
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	config, err := searchConfig()
	if err != nil {
		return err
	}
	key := strings.TrimSpace(*apiKey)
	if key == "" {
		key = os.Getenv("BUILDBUDDY_API_KEY")
	}
	if key == "" {
		return fmt.Errorf("missing --api_key or BUILDBUDDY_API_KEY")
	}
	if *pageSize <= 0 {
		return fmt.Errorf("--page_size must be positive")
	}
	workerCount := *jobs
	if workerCount <= 0 {
		return fmt.Errorf("--jobs must be positive")
	}
	if workerCount > maxJobs {
		workerCount = maxJobs
	}

	start, stop, err := invocationWindow()
	if err != nil {
		return err
	}

	ctx := context.Background()
	ctx = metadata.AppendToOutgoingContext(ctx, authutil.APIKeyHeader, key)

	tenv, err := newToolEnv()
	if err != nil {
		return err
	}
	defer tenv.conn.Close()

	inputRoot, cleanup, err := prepareInputRoot(config)
	if err != nil {
		return err
	}
	defer cleanup()

	log.Infof("Uploading buildgrep input root")
	inputRootDigest, _, err := cachetools.UploadDirectoryToCAS(ctx, tenv.env, *remoteInstanceName, digestFunction, inputRoot)
	if err != nil {
		return fmt.Errorf("upload input root: %w", err)
	}

	log.Infof("Searching invocations updated from %s to %s", start.Format(time.RFC3339), stop.Format(time.RFC3339))
	batches := make(chan []batchInvocation)
	var outputMu sync.Mutex
	eg, egctx := errgroup.WithContext(ctx)
	for range workerCount {
		eg.Go(func() error {
			for {
				select {
				case <-egctx.Done():
					return egctx.Err()
				case batch, ok := <-batches:
					if !ok {
						return nil
					}
					stdout, stderr, err := runRemoteBatch(egctx, tenv.env, inputRootDigest, key, batch)
					outputMu.Lock()
					if len(stderr) > 0 {
						_, _ = os.Stderr.Write(stderr)
					}
					if len(stdout) > 0 {
						_, _ = os.Stdout.Write(stdout)
					}
					outputMu.Unlock()
					if err != nil {
						return err
					}
				}
			}
		})
	}
	eg.Go(func() error {
		defer close(batches)
		return enqueueInvocationBatches(egctx, tenv.client, start, stop, batches)
	})
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

func searchConfig() (*workerConfig, error) {
	args := flag.Args()
	if len(args) == 0 {
		return nil, fmt.Errorf("missing search pattern")
	}
	if len(args) > 1 {
		return nil, fmt.Errorf("expected one search pattern, got %d", len(args))
	}
	pattern := args[0]
	if pattern == "" {
		return nil, fmt.Errorf("search pattern must not be empty")
	}
	if *regex {
		if _, err := regexp.Compile(pattern); err != nil {
			return nil, fmt.Errorf("compile --re pattern: %w", err)
		}
	}
	if _, err := template.New("match").Parse(*outputTemplate); err != nil {
		return nil, fmt.Errorf("parse --template: %w", err)
	}
	return &workerConfig{
		Pattern:  pattern,
		Regex:    *regex,
		Template: *outputTemplate,
	}, nil
}

func invocationWindow() (time.Time, time.Time, error) {
	var (
		stop time.Time
		err  error
	)
	if *stopTimeFlag == "" {
		stop = time.Now()
	} else {
		stop, err = parseTime(*stopTimeFlag)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("parse --stop: %w", err)
		}
	}

	var start time.Time
	if *startTimeFlag == "" {
		if *lookback <= 0 {
			return time.Time{}, time.Time{}, fmt.Errorf("--lookback must be positive")
		}
		start = stop.Add(-*lookback)
	} else {
		start, err = parseTime(*startTimeFlag)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("parse --start: %w", err)
		}
	}
	if !start.Before(stop) {
		return time.Time{}, time.Time{}, fmt.Errorf("--start must be before --stop")
	}
	return start, stop, nil
}

func parseTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	unixSeconds, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("expected RFC3339 or unix seconds")
	}
	return time.Unix(unixSeconds, 0), nil
}

func newToolEnv() (*toolEnv, error) {
	conn, err := grpc_client.DialSimple(*target)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", *target, err)
	}
	env := real_environment.NewBatchEnv()
	env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	env.SetRemoteExecutionClient(repb.NewExecutionClient(conn))
	return &toolEnv{
		env:    env,
		conn:   conn,
		client: bbspb.NewBuildBuddyServiceClient(conn),
	}, nil
}

func prepareInputRoot(config *workerConfig) (string, func(), error) {
	dir, err := os.MkdirTemp("", "buildgrep-input-")
	if err != nil {
		return "", nil, fmt.Errorf("create input root: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(dir) }
	if err := copyRunfile(workerRlocationpath, filepath.Join(dir, "buildgrep_worker")); err != nil {
		cleanup()
		return "", nil, err
	}
	configBytes, err := json.Marshal(config)
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("marshal config: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.json"), configBytes, 0644); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("write config: %w", err)
	}
	return dir, cleanup, nil
}

func copyRunfile(rlocationpath, dst string) error {
	src, err := runfiles.Rlocation(rlocationpath)
	if err != nil {
		return fmt.Errorf("rlocation %q: %w", rlocationpath, err)
	}
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open %q: %w", src, err)
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		return fmt.Errorf("create %q: %w", dst, err)
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return fmt.Errorf("copy %q: %w", src, err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("close %q: %w", dst, err)
	}
	return nil
}

func enqueueInvocationBatches(ctx context.Context, client bbspb.BuildBuddyServiceClient, start, stop time.Time, batches chan<- []batchInvocation) error {
	pending := make([]batchInvocation, 0, batchSize)
	pageToken := ""
	total := 0
	for {
		rsp, err := client.SearchInvocation(ctx, &inpb.SearchInvocationRequest{
			Query: &inpb.InvocationQuery{
				UpdatedAfter:  timestamppb.New(start),
				UpdatedBefore: timestamppb.New(stop),
			},
			Sort: &inpb.InvocationSort{
				SortField: inpb.InvocationSort_UPDATED_AT_USEC_SORT_FIELD,
				Ascending: true,
			},
			Count:     int32(*pageSize),
			PageToken: pageToken,
		})
		if err != nil {
			return fmt.Errorf("search invocations: %w", err)
		}
		for _, inv := range rsp.GetInvocation() {
			iid := inv.GetInvocationId()
			if iid == "" {
				continue
			}
			pending = append(pending, batchInvocation{
				InvocationID:  iid,
				UpdatedAtUsec: inv.GetUpdatedAtUsec(),
			})
			total++
			if len(pending) == batchSize {
				if err := sendBatch(ctx, batches, pending); err != nil {
					return err
				}
				pending = make([]batchInvocation, 0, batchSize)
			}
		}
		if rsp.GetNextPageToken() == "" {
			break
		}
		pageToken = rsp.GetNextPageToken()
	}
	if len(pending) > 0 {
		if err := sendBatch(ctx, batches, pending); err != nil {
			return err
		}
	}
	log.Infof("Queued %d invocations", total)
	return nil
}

func sendBatch(ctx context.Context, batches chan<- []batchInvocation, batch []batchInvocation) error {
	cp := append([]batchInvocation(nil), batch...)
	select {
	case batches <- cp:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func runRemoteBatch(ctx context.Context, env *real_environment.RealEnv, inputRootDigest *repb.Digest, key string, invocations []batchInvocation) ([]byte, []byte, error) {
	invocationsJSON, err := json.Marshal(invocations)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal batch invocations: %w", err)
	}
	cmd := &repb.Command{
		Arguments: []string{
			"./buildgrep_worker",
			"--target=" + *target,
			"--config_file=config.json",
		},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: invocationsEnv, Value: string(invocationsJSON)},
		},
	}
	action := &repb.Action{
		DoNotCache:      true,
		InputRootDigest: inputRootDigest,
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: platform.OperatingSystemPropertyName, Value: platform.LinuxOperatingSystemName},
				{Name: platform.CPUArchitecturePropertyName, Value: platform.AMD64ArchitectureName},
				{Name: platform.EstimatedComputeUnitsPropertyName, Value: "8"},
				{Name: "network", Value: "external"},
			},
		},
		Timeout: durationpb.New(*remoteTimeout),
	}
	arn, err := rexec.Prepare(ctx, env, *remoteInstanceName, digestFunction, action, cmd, "")
	if err != nil {
		return nil, nil, fmt.Errorf("prepare remote batch: %w", err)
	}
	execCtx := platform.WithRemoteHeaderOverride(ctx, platform.SecretEnvOverridesPropertyName, "BUILDBUDDY_API_KEY="+key)
	stream, err := rexec.Start(execCtx, env, arn)
	if err != nil {
		return nil, nil, fmt.Errorf("start remote batch: %w", err)
	}
	rsp, err := rexec.Wait(stream)
	if err != nil {
		return nil, nil, fmt.Errorf("wait remote batch: %w", err)
	}
	if rsp.Err != nil {
		return nil, nil, fmt.Errorf("execute remote batch: %w", rsp.Err)
	}
	if rsp.ExecuteResponse == nil || rsp.ExecuteResponse.GetResult() == nil {
		return nil, nil, fmt.Errorf("remote batch did not return a result")
	}
	result, err := rexec.GetResult(ctx, env, *remoteInstanceName, digestFunction, rsp.ExecuteResponse.GetResult())
	if err != nil {
		return nil, nil, fmt.Errorf("get remote batch result: %w", err)
	}
	if result.ExitCode != 0 {
		return result.Stdout, result.Stderr, fmt.Errorf("remote batch exited with code %d", result.ExitCode)
	}
	return result.Stdout, result.Stderr, nil
}
