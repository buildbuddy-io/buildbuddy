package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/clientidentity"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ocicache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ocimanifest"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_publisher"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/capabilities_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/canary"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/shlex"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	clpb "github.com/buildbuddy-io/buildbuddy/proto/command_line"
	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
	gcrtypes "github.com/google/go-containerregistry/pkg/v1/types"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
)

var (
	// You probably will want to set these (source and target).
	sourceExecutor           = flag.String("source_executor", "", "The backend to replay an action against.")
	sourceAPIKey             = flag.String("source_api_key", "", "The API key of the account that owns the action.")
	targetExecutor           = flag.String("target_executor", "", "The backend to replay an action against.")
	targetAPIKey             = flag.String("target_api_key", "", "API key to use for the target executor.")
	targetRemoteInstanceName = flag.String("target_remote_instance_name", "", "The remote instance name used in the source action")

	// You will need to set these if replaying executions with private images.
	// You will also likely need to set:
	//
	// --target_headers=x-buildbuddy-platform.container-registry-bypass=true
	// --oci.cache.secret=... # should match source/target cache secret
	//
	// So that the image is pulled only from the cache, skipping the registry.
	// This requires using a server admin group API key (see
	// 'auth.admin_group_id'), and the key must have both ORG_ADMIN and
	// CACHE_WRITE capabilities. Note: these cannot be created from the UI
	// currently - must add the capabilities manually in the DB.
	sourceClientIdentityKey = flag.String("source_client_identity_key", "", "The client identity key to use for the source execution service.")
	targetClientIdentityKey = flag.String("target_client_identity_key", "", "The client identity key to use for the target execution service.")

	// Set target_bundle to export all action metadata and transitive
	// dependencies to a local directory. Then, set source_bundle on a
	// subsequent run to replay from a previously captured bundle.
	sourceBundle = flag.String("source_bundle", "", "The directory containing replay data to replay from.")
	targetBundle = flag.String("target_bundle", "", "The directory to write replay data to.")

	// If not using source_bundle, set one of execution_id, invocation_id, or
	// action_digest + source_remote_instance_name.
	executionIDs = flag.Slice("execution_id", []string{}, "Execution IDs to replay. Can be specified more than once.")
	invocationID = flag.String("invocation_id", "", "Invocation ID to replay all actions from.")

	toolInvocationID = flag.String("tool_invocation_id", "", "If set, create an Invocation for the replay_action tool itself. This is useful for querying results from ClickHouse after the replay is complete. Must be a `uuid` - generate with --tool_invocation_id=$(uuidgen | tr '[[:upper:]]' '[[:lower:]]')")

	actionDigest             = flag.String("action_digest", "", "The digest of the action you want to replay.")
	sourceRemoteInstanceName = flag.String("source_remote_instance_name", "", "The remote instance name used in the source action")

	showOutput = flag.Bool("show_output", true, "Show stdout/stderr from the action.")

	// Less common options below.
	overrideCommand   = flag.String("override_command", "", "If set, run this script (with 'sh -c') instead of the original action command line. All other properties such as environment variables and platform properties will be preserved from the original command. The original command can be referenced via the $ORIGINAL_COMMAND environment variable.")
	targetHeaders     = flag.Slice("target_headers", []string{}, "A list of headers to set (format: 'key=val'")
	skipCopyFiles     = flag.Bool("skip_copy_files", false, "Skip copying files to the target. May be useful for speeding up replays after running the script at least once.")
	n                 = flag.Int("n", 1, "Number of times to replay each execution. By default they'll be replayed in serial. Set --jobs to 2 or higher to run concurrently.")
	jobs              = flag.Int("jobs", 1, "Max number of concurrent jobs that can execute actions at once.")
	outDir            = flag.String("out_dir", "", "Dir for writing results and artifacts for each action. If unset, a new temp directory is created and outputs are written there.")
	slowTaskThreshold = flag.Duration("slow_task_threshold", 10*time.Minute, "If set, log a warning if the execution takes longer than this duration.")
	printStats        = flag.Bool("print_stats", false, "If set, print aggregated timing statistics (mean and standard deviation) at the end of the run.")
)

const (
	localCacheClientIdentityKey = "localhost"
	bundleManifestFileName      = "bundle_manifest.json"
)

// ExecutionTimingStats holds timing information from a single execution.
type ExecutionTimingStats struct {
	QueueMs   int64
	FetchMs   int64
	ExecMs    int64
	UploadMs  int64
	CPUMillis int64
}

type StatsCollector struct {
	mu    sync.Mutex
	stats []ExecutionTimingStats
}

func (s *StatsCollector) Add(stats ExecutionTimingStats) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats = append(s.stats, stats)
}

func (s *StatsCollector) Stats() []ExecutionTimingStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

func meanAndStdDev(values []int64) (mean, stdDev float64) {
	if len(values) == 0 {
		return 0, 0
	}
	var sum int64
	for _, v := range values {
		sum += v
	}
	mean = float64(sum) / float64(len(values))
	if len(values) == 1 {
		return mean, 0
	}
	var variance float64
	for _, v := range values {
		diff := float64(v) - mean
		variance += diff * diff
	}
	variance /= float64(len(values) - 1)
	stdDev = math.Sqrt(variance)
	return mean, stdDev
}

func printAggregatedStats(collector *StatsCollector) {
	stats := collector.Stats()
	if len(stats) == 0 {
		return
	}

	queueMs := make([]int64, len(stats))
	fetchMs := make([]int64, len(stats))
	execMs := make([]int64, len(stats))
	uploadMs := make([]int64, len(stats))
	cpuMs := make([]int64, len(stats))

	for i, s := range stats {
		queueMs[i] = s.QueueMs
		fetchMs[i] = s.FetchMs
		execMs[i] = s.ExecMs
		uploadMs[i] = s.UploadMs
		cpuMs[i] = s.CPUMillis
	}

	qMean, qStd := meanAndStdDev(queueMs)
	fMean, fStd := meanAndStdDev(fetchMs)
	eMean, eStd := meanAndStdDev(execMs)
	uMean, uStd := meanAndStdDev(uploadMs)
	cMean, cStd := meanAndStdDev(cpuMs)

	log.Infof("")
	log.Infof("=== Aggregated Timing Statistics (n=%d) ===", len(stats))
	log.Infof("           %10s  %10s", "Mean (ms)", "StdDev (ms)")
	log.Infof("Queue:     %10.1f  %10.1f", qMean, qStd)
	log.Infof("Fetch:     %10.1f  %10.1f", fMean, fStd)
	log.Infof("Exec:      %10.1f  %10.1f", eMean, eStd)
	log.Infof("Upload:    %10.1f  %10.1f", uMean, uStd)
	log.Infof("CPU Total: %10.1f  %10.1f", cMean, cStd)
}

// Example usage:
// $ bazel run //enterprise/tools/replay_action:replay_action -- \
//   --source_executor="grpcs://remote.buildbuddy.io" \
//   --action_digest="blake3/f31e59431cdc5d631853e28151fb664f859b5f4c5dc94f0695408a6d31b84724/142"
//

func diffTimeProtos(start, end *tspb.Timestamp) time.Duration {
	return end.AsTime().Sub(start.AsTime())
}

func logExecutionMetadata(ctx context.Context, md *repb.ExecutedActionMetadata) ExecutionTimingStats {
	qTime := diffTimeProtos(md.GetQueuedTimestamp(), md.GetWorkerStartTimestamp())
	fetchTime := diffTimeProtos(md.GetInputFetchStartTimestamp(), md.GetInputFetchCompletedTimestamp())
	execTime := diffTimeProtos(md.GetExecutionStartTimestamp(), md.GetExecutionCompletedTimestamp())
	uploadTime := diffTimeProtos(md.GetOutputUploadStartTimestamp(), md.GetOutputUploadCompletedTimestamp())
	cpuMillis := md.GetUsageStats().GetCpuNanos() / 1e6
	log.CtxInfof(ctx, "Completed [queue: %4dms, fetch: %4dms, exec: %4dms, upload: %4dms, cpu_total: %4dms]",
		qTime.Milliseconds(), fetchTime.Milliseconds(), execTime.Milliseconds(), uploadTime.Milliseconds(), cpuMillis)
	return ExecutionTimingStats{
		QueueMs:   qTime.Milliseconds(),
		FetchMs:   fetchTime.Milliseconds(),
		ExecMs:    execTime.Milliseconds(),
		UploadMs:  uploadTime.Milliseconds(),
		CPUMillis: cpuMillis,
	}
}

func copyFile(srcCtx, targetCtx context.Context, fmb *FindMissingBatcher, to, from bspb.ByteStreamClient, sourceRN *digest.CASResourceName) error {
	targetRN := digest.NewCASResourceName(sourceRN.GetDigest(), *targetRemoteInstanceName, sourceRN.GetDigestFunction())
	exists, err := fmb.Exists(targetCtx, targetRN.GetDigest())
	if err != nil {
		return status.WrapError(err, "check if file exists")
	}
	if exists {
		log.Debugf("Copy %s: already exists", digest.String(targetRN.GetDigest()))
		return nil
	}
	buf := &bytes.Buffer{}
	if err := cachetools.GetBlob(srcCtx, from, sourceRN, buf); err != nil {
		return status.WrapError(err, "get blob")
	}
	seekBuf := bytes.NewReader(buf.Bytes())
	d2, _, err := cachetools.UploadFromReader(targetCtx, to, targetRN, seekBuf)
	if err != nil {
		return status.WrapError(err, "upload blob")
	}
	if d2.GetHash() != sourceRN.GetDigest().GetHash() || d2.GetSizeBytes() != sourceRN.GetDigest().GetSizeBytes() {
		return status.FailedPreconditionErrorf("copyFile mismatch: %s != %s", digest.String(d2), digest.String(sourceRN.GetDigest()))
	}
	log.Infof("Copied input %s", digest.String(sourceRN.GetDigest()))
	return nil
}

func (r *Replayer) copyTree(srcCtx, targetCtx context.Context, sourceRemoteInstanceName string, tree *repb.Tree, digestType repb.DigestFunction_Value) error {
	eg, ctx := errgroup.WithContext(srcCtx)
	eg.SetLimit(100)
	copyDir := func(dir *repb.Directory) {
		eg.Go(func() error {
			_, err := cachetools.UploadProto(targetCtx, r.destBSClient, *targetRemoteInstanceName, digestType, dir)
			return err
		})
		for _, file := range dir.GetFiles() {
			eg.Go(func() error {
				rn := digest.NewCASResourceName(file.GetDigest(), sourceRemoteInstanceName, digestType)
				dedupeKey := "cas:" + rn.DownloadString()
				return r.doOnce(ctx, dedupeKey, func(ctx context.Context) error {
					return copyFile(srcCtx, targetCtx, r.fmb, r.destBSClient, r.sourceBSClient, rn)
				})
			})
		}
	}
	copyDir(tree.GetRoot())
	for _, dir := range tree.GetChildren() {
		copyDir(dir)
	}
	return eg.Wait()
}

func fetchStdoutOrStderr(ctx context.Context, from bspb.ByteStreamClient, d *repb.Digest, digestType repb.DigestFunction_Value, runDir, name string) error {
	buf := &bytes.Buffer{}
	if d.GetSizeBytes() != 0 {
		ind := digest.NewCASResourceName(d, *targetRemoteInstanceName, digestType)
		if err := cachetools.GetBlob(ctx, from, ind, buf); err != nil {
			return err
		}
	}

	// Print stdout/stderr to the terminal only if we're not running
	// concurrently (to avoid clobbering).
	if *showOutput && buf.String() != "" && *jobs == 1 {
		log.Infof("%s:\n%s", name, buf.String())
	}

	if err := os.WriteFile(filepath.Join(runDir, name), buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("write file: %w", err)
	}

	return nil
}

func getClients(target string) (io.Closer, bspb.ByteStreamClient, repb.ExecutionClient, repb.ContentAddressableStorageClient, repb.ActionCacheClient, error) {
	conn, err := grpc_client.DialSimple(target)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	bs := bspb.NewByteStreamClient(conn)
	exec := repb.NewExecutionClient(conn)
	cas := repb.NewContentAddressableStorageClient(conn)
	ac := repb.NewActionCacheClient(conn)
	return conn, bs, exec, cas, ac, nil
}

func inCopyMode(sourceRemoteInstanceName string) bool {
	return (*targetExecutor != "" && *targetExecutor != *sourceExecutor) ||
		*targetRemoteInstanceName != sourceRemoteInstanceName ||
		*targetAPIKey != *sourceAPIKey
}

func getOutgoingSourceContext(ctx context.Context) (context.Context, error) {
	if *sourceAPIKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *sourceAPIKey)
	}
	sourceClientIdentityKey := *sourceClientIdentityKey
	if sourceClientIdentityKey == "" && *targetClientIdentityKey != "" && *sourceExecutor == *targetExecutor {
		// If the source and target executors are the same, and the target
		// client identity key is set, use it for the source as well.
		sourceClientIdentityKey = *targetClientIdentityKey
	} else if *sourceBundle != "" {
		sourceClientIdentityKey = localCacheClientIdentityKey
	}
	if sourceClientIdentityKey != "" {
		var err error
		ctx, err = setClientIdentity(ctx, sourceClientIdentityKey)
		if err != nil {
			return ctx, fmt.Errorf("set source client identity: %s", err)
		}
	}
	return ctx, nil
}

func getOutgoingTargetContext(ctx context.Context) (context.Context, error) {
	if *targetAPIKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *targetAPIKey)
	}
	targetClientIDKey := *targetClientIdentityKey
	if targetClientIDKey == "" && *sourceClientIdentityKey != "" && *sourceExecutor == *targetExecutor {
		// If the source and target executors are the same, and the source
		// client identity key is set, use it for the target as well.
		targetClientIDKey = *sourceClientIdentityKey
	} else if *targetBundle != "" {
		targetClientIDKey = localCacheClientIdentityKey
	}
	if targetClientIDKey != "" {
		var err error
		ctx, err = setClientIdentity(ctx, targetClientIDKey)
		if err != nil {
			return ctx, fmt.Errorf("set target client identity: %s", err)
		}
	}

	for _, targetHeader := range *targetHeaders {
		pair := strings.SplitN(targetHeader, "=", 2)
		if len(pair) != 2 {
			return ctx, fmt.Errorf("target headers must be of form key=val, got: %q", targetHeader)
		}
		ctx = metadata.AppendToOutgoingContext(ctx, pair[0], pair[1])
	}

	return ctx, nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

// run is the real main function; [main] does nothing but call this func and log
// + exit with any error that it returns.
func run() error {
	rootCtx := context.Background()
	rootCtx, stop := signal.NotifyContext(rootCtx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Configure flags.
	if err := configure(); err != nil {
		return fmt.Errorf("configure: %w", err)
	}

	// Init source/dest contexts with appropriate gRPC headers applied.
	srcCtx, err := getOutgoingSourceContext(rootCtx)
	if err != nil {
		return fmt.Errorf("set source credentials: %s", err)
	}
	targetCtx, err := getOutgoingTargetContext(rootCtx)
	if err != nil {
		return fmt.Errorf("set target credentials: %s", err)
	}

	if *toolInvocationID == "" {
		// Run the tool without wrapping it with an invocation stream (replay
		// results will not be stored in ClickHouse or visible in the UI)
		return runTool(rootCtx, srcCtx, targetCtx)
	}

	// Init the invocation stream, then run the tool under the stream, streaming
	// results via the invocation stream so the results can be queried from
	// ClickHouse. Use the target context so the BES stream is authorized as the
	// same user replaying the invocation.
	pub, err := startToolInvocation(targetCtx)
	if err != nil {
		return fmt.Errorf("start tool invocation: %s", err)
	}
	defer func() {
		if err := pub.FinishWithError(err); err != nil {
			log.Errorf("Failed to finalize build event stream for tool invocation: %s", err)
		}
	}()
	return runTool(rootCtx, srcCtx, targetCtx)
}

// Initializes flag defaults, parses flags, and validates flag constraints.
func configure() error {
	// When serving from a bundle, default grpc_port to avoid conflicts with
	// default local app port.
	if err := flagutil.SetValueForFlagName("grpc_port", 11985, nil, false); err != nil {
		return fmt.Errorf("set default grpc_port: %s", err)
	}
	flag.Parse()
	if err := log.Configure(); err != nil {
		return fmt.Errorf("configure logging: %w", err)
	}
	if *targetBundle != "" && (*targetExecutor != "" || *targetAPIKey != "" || *targetClientIdentityKey != "") {
		return fmt.Errorf("Cannot set both target_bundle and other target_* flags")
	}
	if *targetBundle == "" && *targetExecutor == "" {
		return fmt.Errorf("Must set either target_bundle or target_executor")
	}
	if *sourceBundle != "" && (*sourceExecutor != "" || *sourceAPIKey != "" || *sourceClientIdentityKey != "") {
		return fmt.Errorf("Cannot set both source_bundle and other source_* flags")
	}
	if *sourceBundle == "" && *sourceExecutor == "" {
		return fmt.Errorf("Must set either source_bundle or source_executor")
	}
	if *sourceBundle != "" && *targetBundle != "" {
		return fmt.Errorf("Cannot set both source_bundle and target_bundle (use 'cp -r' to copy bundles)")
	}
	if *sourceAPIKey != "" && *targetAPIKey == "" {
		log.Warningf("--target_api_key is not set, but --source_api_key was set. Replaying as anonymous user.")
	}
	if *outDir == "" {
		tmp, err := os.MkdirTemp("", "replay-action-*")
		if err != nil {
			return fmt.Errorf("create output dir: %s", err)
		}
		log.Infof("Writing results to %s", tmp)
		*outDir = tmp
	}
	return nil
}

// runTool should be called after configure() and after initializing the tool's
// BES stream if applicable. It contains the main logic from the tool,
// initializing the source/dest clients, copying inputs, then replaying
// executions.
func runTool(rootCtx, srcCtx, targetCtx context.Context) error {
	// If we are replaying from a saved bundle, transparently start a cache
	// server with the bundle as the cache root directory, and set
	// --source_executor to point to the bundle server. This simplifies the
	// copying logic quite a bit, and works because the source executor is only
	// needed for the cache APIs (copying inputs).
	if *sourceBundle != "" {
		localCache, err := startCacheServerForBundle(*sourceBundle)
		if err != nil {
			return fmt.Errorf("start local cache for source bundle: %w", err)
		}
		defer localCache.Shutdown()
		*sourceExecutor = localCache.GRPCTarget()
	}
	// Same for target bundle - start a cache server to serve as the
	// --target_executor, and copy files to it. Note, the replay logic will skip
	// actually trying to execute - executions will be written to the target
	// bundle manifest, for later replaying via --source_bundle, but not
	// actually executed.
	if *targetBundle != "" {
		localCache, err := startCacheServerForBundle(*targetBundle)
		if err != nil {
			return fmt.Errorf("start local cache for target bundle: %w", err)
		}
		defer localCache.Shutdown()
		*targetExecutor = localCache.GRPCTarget()
	}

	log.Infof("Connecting to source %q", *sourceExecutor)
	sourceConn, sourceBSClient, _, sourceCASClient, sourceACClient, err := getClients(*sourceExecutor)
	if err != nil {
		return fmt.Errorf("connect to source: %w", err)
	}
	defer sourceConn.Close()

	log.Infof("Connecting to target %q", *targetExecutor)
	destConn, destBSClient, execClient, destCASClient, destACClient, err := getClients(*targetExecutor)
	if err != nil {
		return fmt.Errorf("connect to target: %w", err)
	}
	defer destConn.Close()

	var statsCollector *StatsCollector
	if *printStats {
		statsCollector = &StatsCollector{}
	}
	replayer := &Replayer{
		sourceBSClient:  sourceBSClient,
		sourceCASClient: sourceCASClient,
		sourceACClient:  sourceACClient,
		destBSClient:    destBSClient,
		destCASClient:   destCASClient,
		destACClient:    destACClient,
		execClient:      execClient,
		statsCollector:  statsCollector,
	}
	replayer.uploadGroup.SetLimit(3)
	replayer.executeGroup.SetLimit(*jobs)

	// Get the source executions to be replayed.
	sourceExecutions, err := getSourceExecutions(srcCtx)
	if err != nil {
		return fmt.Errorf("get source executions: %w", err)
	}

	// Replay the source executions from source -> target using the configured
	// replayer.
	if err := replayAll(rootCtx, srcCtx, targetCtx, replayer, sourceExecutions); err != nil {
		return fmt.Errorf("do replay: %w", err)
	}
	return nil
}

type sourceExecutions struct {
	// resource names to replay.
	resourceNames []*digest.CASResourceName

	// Source metadata (target label, action mnemonic, etc.).
	// Indexed by `rn.DownloadString()` for each resource name in resourceNames.
	// Only populated if replaying from an invocation or source bundle.
	metadata map[string]*Execution
}

// getSourceExecutions returns the resource names and metadata of the executions
// to be replayed. The logic is somewhat involved since the tool is very
// flexible and supports replaying executions from several different sources
// (e.g. an action_digest, execution_id, all executions from an invocation_id,
// or all executions from a source_bundle).
func getSourceExecutions(srcCtx context.Context) (*sourceExecutions, error) {
	var resourceNames []*digest.CASResourceName
	executionMetadata := map[string]*Execution{}

	if *actionDigest != "" {
		// For backwards compatibility, attempt to fixup old style digest
		// strings that don't start with a '/blobs/' prefix.
		digestString := *actionDigest
		if !strings.HasPrefix(digestString, "/blobs") {
			digestString = *sourceRemoteInstanceName + "/blobs/" + digestString
		}
		rn, err := digest.ParseDownloadResourceName(digestString)
		if err != nil {
			return nil, fmt.Errorf("Error parsing action digest %q: %s", *actionDigest, err)
		}
		resourceNames = append(resourceNames, rn)
	}
	for _, executionID := range *executionIDs {
		rn, err := digest.ParseUploadResourceName(executionID)
		if err != nil {
			// Fall back to parsing as download resource name.
			rn, err = digest.ParseDownloadResourceName(executionID)
			if err != nil {
				return nil, fmt.Errorf("Invalid execution ID %q: %s", executionID, err)
			}
		}
		resourceNames = append(resourceNames, rn)
	}

	// If replaying from a bundle, and not filtering to specific execution(s)
	// from the bundle, load execution resource names from the manifest.
	if *sourceBundle != "" {
		bundleManifestBytes, err := os.ReadFile(filepath.Join(*sourceBundle, bundleManifestFileName))
		if err != nil {
			return nil, fmt.Errorf("read bundle manifest: %s", err)
		}
		bundleManifest := &BundleManifest{}
		if err := json.Unmarshal(bundleManifestBytes, bundleManifest); err != nil {
			return nil, fmt.Errorf("unmarshal bundle manifest: %s", err)
		}
		executions := bundleManifest.Executions
		// Sort executions by queued timestamp so that they are replayed
		// *roughly* in their original order.
		sort.Slice(executions, func(i, j int) bool {
			// If the timestamp is missing, assume it's because the invocation
			// was cancelled, and sort the execution last.
			zi := executions[i].QueuedTimestamp.IsZero()
			zj := executions[j].QueuedTimestamp.IsZero()
			if zi != zj {
				return !zi
			}
			return executions[i].QueuedTimestamp.Before(executions[j].QueuedTimestamp)
		})

		// If we're filtering to specific execution IDs, only read those IDs
		// from the manifest.
		keep := make(map[string]bool, len(resourceNames))
		for _, executionID := range resourceNames {
			keep[executionID.DownloadString()] = true
		}

		for _, execution := range executions {
			rn, err := digest.ParseDownloadResourceName(execution.ActionResourceName)
			if err != nil {
				return nil, fmt.Errorf("Invalid action resource name %q: %s", execution.ActionResourceName, err)
			}
			rns := rn.DownloadString()
			if len(keep) > 0 {
				if !keep[rns] {
					continue
				}
			} else {
				resourceNames = append(resourceNames, rn)
			}
			executionMetadata[rns] = &execution
		}
	}

	// If we're replaying from an invocation, load execution resource names from
	// the invocation.
	if *invocationID != "" {
		conn, err := grpc_client.DialSimple(*sourceExecutor)
		if err != nil {
			return nil, fmt.Errorf("Error dialing executor: %s", err.Error())
		}
		defer conn.Close()
		client := bbspb.NewBuildBuddyServiceClient(conn)
		rsp, err := client.GetExecution(srcCtx, &espb.GetExecutionRequest{
			ExecutionLookup: &espb.ExecutionLookup{
				InvocationId: *invocationID,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("Could not retrieve invocation executions: %s", err)
		}
		for _, e := range rsp.Execution {
			rn, err := digest.ParseUploadResourceName(e.ExecutionId)
			if err != nil {
				return nil, fmt.Errorf("Invalid execution ID %q: %s", e.ExecutionId, err)
			}
			resourceNames = append(resourceNames, rn)
			executionMetadata[rn.DownloadString()] = &Execution{
				ActionResourceName:       rn.DownloadString(),
				QueuedTimestamp:          e.GetExecutedActionMetadata().GetQueuedTimestamp().AsTime(),
				WorkerCompletedTimestamp: e.GetExecutedActionMetadata().GetWorkerCompletedTimestamp().AsTime(),
				// Store basic identifying info for more useful reporting.
				TargetLabel:    e.GetTargetLabel(),
				ActionMnemonic: e.GetActionMnemonic(),
			}
		}
	}

	// Validate that we have something to replay.
	if len(resourceNames) == 0 {
		return nil, fmt.Errorf("missing -action_digest or -execution_id or -invocation_id")
	}
	return &sourceExecutions{
		resourceNames: resourceNames,
		metadata:      executionMetadata,
	}, nil
}

// replayAll replays the given source executions from source -> target using the
// given replayer. For each action, this includes (1) copying action inputs and
// (2) executing the action (depending on flags, one or both of these steps may
// be skipped).
func replayAll(rootCtx, srcCtx, targetCtx context.Context, replayer *Replayer, sourceExecutions *sourceExecutions) (err error) {
	resourceNames := sourceExecutions.resourceNames
	executionMetadata := sourceExecutions.metadata

	for _, rn := range resourceNames {
		if err := replayer.Start(rootCtx, srcCtx, targetCtx, rn, executionMetadata[rn.DownloadString()]); err != nil {
			return fmt.Errorf("start replay: %w", err)
		}
	}

	// Wait for all started replay operations to complete.
	if err := replayer.Wait(); err != nil {
		return fmt.Errorf("wait for replay: %w", err)
	}

	// If we've got a target bundle, then at this point we will have copied all
	// files, but won't have written the manifest yet. Do that now.
	if *targetBundle != "" {
		if err := writeTargetBundleManifest(resourceNames, executionMetadata); err != nil {
			return fmt.Errorf("write target bundle manifest: %w", err)
		}
	}

	// Print aggregated stats if enabled.
	if replayer.statsCollector != nil {
		printAggregatedStats(replayer.statsCollector)
	}
	return nil

}

func writeTargetBundleManifest(resourceNames []*digest.CASResourceName, executionMetadata map[string]*Execution) error {
	bundleManifest := &BundleManifest{
		Executions: make([]Execution, 0, len(resourceNames)),
	}
	for _, rn := range resourceNames {
		if md, ok := executionMetadata[rn.DownloadString()]; ok {
			bundleManifest.Executions = append(bundleManifest.Executions, *md)
		} else {
			bundleManifest.Executions = append(bundleManifest.Executions, Execution{
				ActionResourceName: rn.DownloadString(),
			})
		}
	}
	bundleManifestBytes, err := json.MarshalIndent(bundleManifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal bundle manifest: %s", err)
	}
	if err := os.WriteFile(filepath.Join(*targetBundle, bundleManifestFileName), bundleManifestBytes, 0644); err != nil {
		return fmt.Errorf("write bundle manifest: %s", err)
	}
	return nil
}

func setClientIdentity(ctx context.Context, key string) (context.Context, error) {
	for k, v := range map[string]any{
		"app.client_identity.key":    key,
		"app.client_identity.origin": "localhost",
		// TODO: add replay_action as a trusted client and use that instead of
		// "executor", or just trust any client with a signed key.
		"app.client_identity.client": "executor",
		// Set a long expiration in case the script takes a long time to run.
		"app.client_identity.expiration": 6 * time.Hour,
	} {
		if err := flagutil.SetValueForFlagName(k, v, nil, false); err != nil {
			return ctx, fmt.Errorf("set flag %s: %s", k, err)
		}
	}
	service, err := clientidentity.New(clockwork.NewRealClock())
	if err != nil {
		return ctx, fmt.Errorf("initialize client identity: %s", err)
	}
	ctx, err = service.AddIdentityToContext(ctx)
	if err != nil {
		return ctx, fmt.Errorf("add client identity to context: %s", err)
	}
	return ctx, nil
}

type Replayer struct {
	fmb *FindMissingBatcher

	replayGroup  errgroup.Group
	uploadGroup  errgroup.Group
	executeGroup errgroup.Group

	sourceBSClient, destBSClient   bspb.ByteStreamClient
	sourceCASClient, destCASClient repb.ContentAddressableStorageClient
	sourceACClient, destACClient   repb.ActionCacheClient
	execClient                     repb.ExecutionClient

	// Map of upload keys to [func() error] objects which will upload the
	// digest once.
	uploads sync.Map

	// statsCollector collects execution timing stats when print_stats is enabled.
	statsCollector *StatsCollector
}

func (r *Replayer) Start(ctx, srcCtx, targetCtx context.Context, sourceExecutionRN *digest.CASResourceName, md *Execution) error {
	if r.fmb == nil {
		r.fmb = NewFindMissingBatcher(targetCtx, *targetRemoteInstanceName, sourceExecutionRN.GetDigestFunction(), r.destCASClient, FindMissingBatcherOpts{})
	}

	r.replayGroup.Go(func() error {
		return r.replay(ctx, srcCtx, targetCtx, sourceExecutionRN, md)
	})
	return nil
}

func (r *Replayer) Wait() error {
	return r.replayGroup.Wait()
}

func (r *Replayer) replay(ctx, srcCtx, targetCtx context.Context, sourceExecutionRN *digest.CASResourceName, md *Execution) error {
	// Fetch the action to ensure it exists.
	action := &repb.Action{}
	if err := cachetools.GetBlobAsProto(srcCtx, r.sourceBSClient, sourceExecutionRN, action); err != nil {
		return status.WrapErrorf(err, "fetch action %s", sourceExecutionRN.DownloadString())
	}
	// If remote_executor and target_executor are not the same, copy the files.
	if inCopyMode(sourceExecutionRN.GetInstanceName()) && !*skipCopyFiles {
		uploadErr := make(chan error, 1)
		r.uploadGroup.Go(func() error {
			err := r.upload(ctx, srcCtx, targetCtx, action, sourceExecutionRN)
			uploadErr <- err
			return err
		})
		if err := <-uploadErr; err != nil {
			return status.WrapError(err, "copy execution inputs")
		}
	}

	// If we're just dumping action data to a target_bundle, don't actually execute
	// anything.
	if *targetBundle != "" {
		return nil
	}

	return r.execute(ctx, srcCtx, targetCtx, action, sourceExecutionRN, md)
}

func (r *Replayer) doOnce(ctx context.Context, key string, upload func(ctx context.Context) error) error {
	onceFn := sync.OnceValue(func() error {
		return upload(ctx)
	})
	dedupedOnceFn, _ := r.uploads.LoadOrStore(key, onceFn)
	return dedupedOnceFn.(func() error)()
}

func (r *Replayer) upload(ctx, srcCtx, targetCtx context.Context, action *repb.Action, sourceExecutionRN *digest.CASResourceName) error {
	s := sourceExecutionRN.DownloadString()
	log.Infof("Copying data for execution %q", s)
	eg, targetCtx := errgroup.WithContext(targetCtx)
	eg.Go(func() error {
		actionRN := sourceExecutionRN
		if err := copyFile(srcCtx, targetCtx, r.fmb, r.destBSClient, r.sourceBSClient, actionRN); err != nil {
			return status.WrapError(err, "copy action")
		}
		log.CtxDebugf(ctx, "Copied action %s", actionRN.DownloadString())
		return nil
	})
	eg.Go(func() error {
		commandRN := digest.NewCASResourceName(action.GetCommandDigest(), sourceExecutionRN.GetInstanceName(), sourceExecutionRN.GetDigestFunction())
		if err := copyFile(srcCtx, targetCtx, r.fmb, r.destBSClient, r.sourceBSClient, commandRN); err != nil {
			return status.WrapError(err, "copy command")
		}
		log.CtxDebugf(ctx, "Copied command %s", commandRN.DownloadString())
		return nil
	})
	eg.Go(func() error {
		treeRN := digest.NewCASResourceName(action.GetInputRootDigest(), sourceExecutionRN.GetInstanceName(), sourceExecutionRN.GetDigestFunction())
		tree, err := cachetools.GetTreeFromRootDirectoryDigest(srcCtx, r.sourceCASClient, treeRN)
		if err != nil {
			return status.WrapError(err, "GetTree")
		}
		if err := r.copyTree(srcCtx, targetCtx, sourceExecutionRN.GetInstanceName(), tree, sourceExecutionRN.GetDigestFunction()); err != nil {
			return status.WrapError(err, "copy tree")
		}
		log.CtxDebugf(ctx, "Copied input root %s", treeRN.DownloadString())
		return nil
	})
	eg.Go(func() error {
		imageProp := platform.FindValue(action.GetPlatform(), "container-image")
		osProp := platform.FindValue(action.GetPlatform(), "OSFamily")
		archProp := platform.FindValue(action.GetPlatform(), "Arch")
		dedupeKey := fmt.Sprintf("image:%s/%s:%s", osProp, archProp, imageProp)
		return r.doOnce(ctx, dedupeKey, func(ctx context.Context) error {
			if err := r.copyCachedContainerImage(ctx, srcCtx, targetCtx, action); err != nil {
				log.CtxWarningf(ctx, "Error copying image: %s", err)
				return nil
			}
			return nil
		})
	})
	if err := eg.Wait(); err != nil {
		return err
	}
	destName := *targetExecutor
	if *targetBundle != "" {
		destName = fmt.Sprintf("bundle %s", *targetBundle)
	}
	log.Infof("Finished copying files to %s for execution %s", destName, s)
	return nil
}

func (r *Replayer) copyCachedContainerImage(ctx, srcCtx, targetCtx context.Context, action *repb.Action) error {
	imageProp := platform.FindValue(action.GetPlatform(), "container-image")
	if imageProp == "" || imageProp == "none" {
		return nil
	}
	// Parse image
	imageRefStr := strings.TrimPrefix(imageProp, "docker://")
	imageRef, err := gcrname.ParseReference(imageRefStr)
	if err != nil {
		return status.WrapError(err, "parse image")
	}
	digestRef, ok := imageRef.(gcrname.Digest)
	if !ok {
		log.CtxWarningf(ctx, "Image %q does not include a digest - must be resolved via the remote registry. This will fail if the image is private and credentials aren't set.", imageProp)
		return nil
	}
	hash, err := gcr.NewHash(digestRef.DigestStr())
	if err != nil {
		return fmt.Errorf("invalid digest %q: %s", digestRef.DigestStr(), err)
	}
	// Fetch manifest (may be an index manifest or image manifest)
	cachedManifest, err := ocicache.FetchManifestFromAC(srcCtx, r.sourceACClient, digestRef.Repository, hash, imageRef)
	if err != nil {
		log.CtxWarningf(ctx, "Error fetching manifest from AC: %s", err)
		return nil
	}
	// Write manifest (async)
	var eg errgroup.Group
	eg.SetLimit(4)
	eg.Go(func() error {
		if err := ocicache.WriteManifestToAC(targetCtx, cachedManifest.GetRaw(), r.destACClient, digestRef.Repository, hash, cachedManifest.GetContentType(), imageRef); err != nil {
			return status.WrapError(err, "write manifest to AC")
		}
		log.CtxInfof(ctx, "Copied image manifest to AC")
		return nil
	})

	// If we have an index manifest, find the first image manifest matching the
	// target platform, and copy that manifest to cache, since it's what the
	// executor will use. Then proceed to use that manifest for copying layers.
	var rawImageManifest []byte
	mediaType := gcrtypes.MediaType(cachedManifest.GetContentType())
	if mediaType.IsIndex() {
		indexManifest, err := gcr.ParseIndexManifest(bytes.NewReader(cachedManifest.GetRaw()))
		if err != nil {
			return fmt.Errorf("parse index manifest: %s", err)
		}
		platform, err := getImagePlatform(action)
		if err != nil {
			return fmt.Errorf("get image platform: %s", err)
		}
		imageManifestDesc, err := ocimanifest.FindFirstImageManifest(*indexManifest, platform)
		if err != nil {
			return fmt.Errorf("find first image manifest: %s", err)
		}
		cachedImageManifest, err := ocicache.FetchManifestFromAC(srcCtx, r.sourceACClient, digestRef.Repository, imageManifestDesc.Digest, imageRef)
		if err != nil {
			return fmt.Errorf("fetch image manifest from AC: %s", err)
		}
		eg.Go(func() error {
			if err := ocicache.WriteManifestToAC(targetCtx, cachedImageManifest.GetRaw(), r.destACClient, digestRef.Repository, imageManifestDesc.Digest, cachedImageManifest.GetContentType(), imageRef); err != nil {
				return status.WrapError(err, "write manifest to AC")
			}
			log.CtxInfof(ctx, "Copied platform-specific image manifest to AC")
			return nil
		})
		rawImageManifest = cachedImageManifest.GetRaw()
	} else if mediaType.IsImage() {
		rawImageManifest = cachedManifest.GetRaw()
	} else {
		return fmt.Errorf("unexpected manifest type: %s", mediaType)
	}
	imageManifest, err := gcr.ParseManifest(bytes.NewReader(rawImageManifest))
	if err != nil {
		return fmt.Errorf("parse image manifest: %s", err)
	}

	// Now that we've copied manifests, copy blobs.
	copyBlob := func(hash gcr.Hash, contentLength int64, contentType string, label string) error {
		pr, pw := io.Pipe()
		defer pr.Close()
		go func() {
			defer pw.Close()
			if err := ocicache.FetchBlobFromCache(srcCtx, pw, r.sourceBSClient, hash, contentLength); err != nil {
				pw.CloseWithError(fmt.Errorf("read image %s blob %s from source cache: %s", label, hash, err))
			}
		}()
		if err := ocicache.WriteBlobToCache(targetCtx, pr, r.destBSClient, r.destACClient, digestRef.Repository, hash, contentType, contentLength); err != nil {
			return fmt.Errorf("copy image %s blob %s to target cache: %s", label, hash, err)
		}
		log.CtxInfof(ctx, "Copied image %s blob %s", label, hash)
		return nil
	}

	// Copy the config blob (if it's not inlined)
	if len(imageManifest.Config.Data) == 0 && imageManifest.Config.Size > 0 {
		configHash, err := gcr.NewHash(imageManifest.Config.Digest.String())
		if err != nil {
			return fmt.Errorf("parse config digest: %s", err)
		}
		eg.Go(func() error {
			return copyBlob(configHash, imageManifest.Config.Size, string(imageManifest.Config.MediaType), "config")
		})
	}

	// Copy all layer blobs
	for _, layer := range imageManifest.Layers {
		if layer.Size <= 0 {
			continue
		}
		layerHash, err := gcr.NewHash(layer.Digest.String())
		if err != nil {
			return fmt.Errorf("parse layer digest %q: %s", layer.Digest.String(), err)
		}
		eg.Go(func() error {
			return copyBlob(layerHash, layer.Size, string(layer.MediaType), "layer")
		})
	}

	return eg.Wait()
}

func getImagePlatform(action *repb.Action) (gcr.Platform, error) {
	partialTask := &repb.ExecutionTask{Action: action}
	props, err := platform.ParseProperties(partialTask)
	if err != nil {
		return gcr.Platform{}, err
	}
	return gcr.Platform{
		OS:           props.OS,
		Architecture: props.Arch,
	}, nil
}

func (r *Replayer) execute(ctx, srcCtx, targetCtx context.Context, action *repb.Action, sourceExecutionRN *digest.CASResourceName, md *Execution) error {
	// If we're overriding the command, do that now.
	if *overrideCommand != "" {
		// Download the command and update arguments.
		sourceCRN := digest.NewCASResourceName(action.GetCommandDigest(), sourceExecutionRN.GetInstanceName(), sourceExecutionRN.GetDigestFunction())
		cmd := &repb.Command{}
		if err := cachetools.GetBlobAsProto(srcCtx, r.sourceBSClient, sourceCRN, cmd); err != nil {
			return fmt.Errorf("get command: %s", err)
		}
		// Set ORIGINAL_COMMAND in env to the original command, shell-quoted.
		// This way it can be wrapped with other commands, e.g.
		// --override_command='strace $ORIGINAL_COMMAND'
		cmd.EnvironmentVariables = append(cmd.EnvironmentVariables, &repb.Command_EnvironmentVariable{
			Name:  "ORIGINAL_COMMAND",
			Value: shlex.Quote(cmd.Arguments...),
		})
		cmd.Arguments = []string{"sh", "-c", *overrideCommand}

		// Upload the new command and action.
		cd, err := cachetools.UploadProto(targetCtx, r.destBSClient, *targetRemoteInstanceName, sourceExecutionRN.GetDigestFunction(), cmd)
		if err != nil {
			return fmt.Errorf("upload new command: %s", err)
		}
		action = action.CloneVT()
		action.CommandDigest = cd
		ad, err := cachetools.UploadProto(targetCtx, r.destBSClient, *targetRemoteInstanceName, sourceExecutionRN.GetDigestFunction(), action)
		if err != nil {
			return status.WrapError(err, "upload new action")
		}

		sourceExecutionRN = digest.NewCASResourceName(ad, *targetRemoteInstanceName, sourceExecutionRN.GetDigestFunction())
	}
	// If we're running multiple executions of the action, and the do_not_cache
	// field isn't set, then the action merger is likely to dedupe some of the
	// executions. So we need to override the do_not_cache field to true in
	// order to force execution.
	if *n > 1 && !action.GetDoNotCache() {
		ctx = log.EnrichContext(ctx, "original_execution", sourceExecutionRN.DownloadString())
		targetCtx = log.EnrichContext(targetCtx, "original_execution", sourceExecutionRN.DownloadString())

		// Update field
		action = action.CloneVT()
		action.DoNotCache = true

		// Upload the new action.
		ad, err := cachetools.UploadProto(targetCtx, r.destBSClient, *targetRemoteInstanceName, sourceExecutionRN.GetDigestFunction(), action)
		if err != nil {
			return status.WrapError(err, "upload new action")
		}

		sourceExecutionRN = digest.NewCASResourceName(ad, *targetRemoteInstanceName, sourceExecutionRN.GetDigestFunction())
	}

	execReq := &repb.ExecuteRequest{
		InstanceName:    *targetRemoteInstanceName,
		SkipCacheLookup: true,
		ActionDigest:    sourceExecutionRN.GetDigest(),
		DigestFunction:  sourceExecutionRN.GetDigestFunction(),
	}
	executeErr := make(chan error, *n)
	for i := 1; i <= *n; i++ {
		i := i
		r.executeGroup.Go(func() error {
			err := execute(targetCtx, r.execClient, r.destBSClient, i, sourceExecutionRN, execReq, md, action, r.statsCollector)
			executeErr <- err
			return err
		})
	}
	var lastErr error
	for i := 1; i <= *n; i++ {
		if err := <-executeErr; err != nil {
			log.CtxWarningf(ctx, "Execution %d failed: %s", i, err)
			lastErr = err
		}
	}
	return lastErr
}

func execute(ctx context.Context, execClient repb.ExecutionClient, bsClient bspb.ByteStreamClient, i int, sourceExecutionID *digest.CASResourceName, req *repb.ExecuteRequest, md *Execution, action *repb.Action, statsCollector *StatsCollector) error {
	ctx = log.EnrichContext(ctx, "run", fmt.Sprintf("%d/%d", i, *n))

	actionId := sourceExecutionID.GetDigest().GetHash()
	rmd := &repb.RequestMetadata{
		ActionId:         actionId,
		ToolInvocationId: *toolInvocationID,
	}
	if md != nil {
		rmd.ActionMnemonic = md.ActionMnemonic
		rmd.TargetId = md.TargetLabel
		ctx = log.EnrichContext(ctx, "target", md.TargetLabel)
		ctx = log.EnrichContext(ctx, "mnemonic", md.ActionMnemonic)
	}
	ctx, err := bazel_request.WithRequestMetadata(ctx, rmd)
	if err != nil {
		return status.WrapError(err, "set request metadata")
	}
	log.CtxInfof(ctx, "Starting execution")
	defer canary.StartWithLateFn(*slowTaskThreshold, func(duration time.Duration) {
		log.CtxWarningf(ctx, "Execution still running after %s", duration)
	}, func(time.Duration) {})()

	stream, err := execClient.Execute(ctx, req)
	if err != nil {
		return status.WrapError(err, "Execute")
	}
	printedExecutionID := false
	runDir := filepath.Join(*outDir, fmt.Sprintf("%s/%d", actionId, i))
	if err := os.MkdirAll(runDir, 0755); err != nil {
		return fmt.Errorf("make run dir %q: %s", runDir, err)
	}
	for {
		op, err := stream.Recv()
		if err != nil {
			return status.WrapError(err, "recv from Execute stream")
		}
		if !printedExecutionID {
			log.CtxInfof(ctx, "Started execution %q", op.GetName())
			printedExecutionID = true
			ctx = log.EnrichContext(ctx, log.ExecutionIDKey, op.GetName())
		}
		log.CtxDebugf(ctx, "Execution stage: %s", operation.ExtractStage(op))
		if op.GetDone() {
			metadata := &repb.ExecuteOperationMetadata{}
			if err := op.GetMetadata().UnmarshalTo(metadata); err == nil {
				jb, _ := (protojson.MarshalOptions{Multiline: true}).Marshal(metadata)
				log.CtxDebugf(ctx, "Metadata: %s", string(jb))
			}

			response := &repb.ExecuteResponse{}
			if err := op.GetResponse().UnmarshalTo(response); err != nil {
				return status.WrapError(err, "unmarshal ExecuteResponse")
			}

			if err := gstatus.ErrorProto(response.GetStatus()); err != nil {
				log.CtxErrorf(ctx, "Execution failed: %s", err)
			}

			jb, _ := (protojson.MarshalOptions{Multiline: true}).Marshal(response)
			log.CtxDebugf(ctx, "ExecuteResponse: %s", string(jb))
			result := response.GetResult()
			if result.GetExitCode() > 0 {
				log.CtxWarningf(ctx, "Action exited with code %d", result.GetExitCode())
			}
			if err := os.WriteFile(filepath.Join(runDir, "execute_response.json"), jb, 0644); err != nil {
				log.CtxWarningf(ctx, "Failed to write response.json: %s", err)
			}
			if err := fetchStdoutOrStderr(ctx, bsClient, result.GetStdoutDigest(), sourceExecutionID.GetDigestFunction(), runDir, "stdout"); err != nil {
				log.CtxWarningf(ctx, "Failed to get stdout: %s", err)
			}
			if err := fetchStdoutOrStderr(ctx, bsClient, result.GetStderrDigest(), sourceExecutionID.GetDigestFunction(), runDir, "stderr"); err != nil {
				log.CtxWarningf(ctx, "Failed to get stderr: %s", err)
			}
			timingStats := logExecutionMetadata(ctx, response.GetResult().GetExecutionMetadata())
			if statsCollector != nil {
				statsCollector.Add(timingStats)
			}
			break
		}
	}
	return nil
}

type findMissingRequest struct {
	Digest       *repb.Digest
	ResponseChan chan findMissingResponse
}

type findMissingResponse struct {
	Missing bool
	Error   error
}

type FindMissingBatcherOpts struct {
	// MaxBatchSize is the maximum number of digests that may be requested as
	// part of a single batch. When the current batch reaches this size, it is
	// immediately flushed.
	MaxBatchSize int
	// MaxBatchingDelay is the maximum duration that any request should have to
	// be queued while it is waiting for other requests to join the batch.
	MaxBatchingDelay time.Duration
	// MaxConcurrency is the max number of goroutines that may be issuing
	// requests at once.
	MaxConcurrency int
}

// FindMissingBatcher provides a convenient way to check whether individual
// digests are missing from cache, while transparently batching requests that
// are issued very close together (temporally) for greater efficiency.
type FindMissingBatcher struct {
	ctx            context.Context
	instanceName   string
	digestFunction repb.DigestFunction_Value
	client         repb.ContentAddressableStorageClient
	opts           FindMissingBatcherOpts
	reqs           chan findMissingRequest
}

func NewFindMissingBatcher(ctx context.Context, instanceName string, digestFunction repb.DigestFunction_Value, client repb.ContentAddressableStorageClient, opts FindMissingBatcherOpts) *FindMissingBatcher {
	if opts.MaxBatchSize == 0 {
		opts.MaxBatchSize = 128
	}
	if opts.MaxBatchingDelay == 0 {
		opts.MaxBatchingDelay = 1 * time.Millisecond
	}
	if opts.MaxConcurrency == 0 {
		opts.MaxConcurrency = 4
	}
	f := &FindMissingBatcher{
		ctx:            ctx,
		instanceName:   instanceName,
		digestFunction: digestFunction,
		client:         client,
		opts:           opts,
		reqs:           make(chan findMissingRequest, 128),
	}
	go f.run(ctx)
	return f
}

func (f *FindMissingBatcher) Exists(ctx context.Context, d *repb.Digest) (bool, error) {
	req := findMissingRequest{
		Digest:       d,
		ResponseChan: make(chan findMissingResponse, 1),
	}
	select {
	case <-f.ctx.Done():
		return false, f.ctx.Err()
	case <-ctx.Done():
		return false, ctx.Err()
	case f.reqs <- req:
	}
	select {
	case <-f.ctx.Done():
		return false, f.ctx.Err()
	case <-ctx.Done():
		return false, ctx.Err()
	case res := <-req.ResponseChan:
		if res.Error != nil {
			return false, res.Error
		}
		return !res.Missing, nil
	}
}

func (f *FindMissingBatcher) run(ctx context.Context) {
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(f.opts.MaxConcurrency)
	var batch []findMissingRequest
	t := time.NewTimer(0)
	for {
		flush := false
		select {
		case <-t.C:
			if len(batch) > 0 {
				flush = true
			}
		case <-ctx.Done():
			return
		case req := <-f.reqs:
			batch = append(batch, req)
			if len(batch) == 1 {
				t.Stop()
				t = time.NewTimer(f.opts.MaxBatchingDelay)
			}
			if len(batch) >= f.opts.MaxBatchSize {
				flush = true
			}
		}
		if flush {
			b := batch
			batch = nil
			eg.Go(func() error { return f.flush(b) })
		}
	}
}

func (f *FindMissingBatcher) flush(batch []findMissingRequest) error {
	responseChansByHash := map[string][]chan findMissingResponse{}
	batchReq := &repb.FindMissingBlobsRequest{
		InstanceName:   f.instanceName,
		DigestFunction: f.digestFunction,
	}
	for _, req := range batch {
		hash := req.Digest.GetHash()
		if _, ok := responseChansByHash[hash]; !ok {
			batchReq.BlobDigests = append(batchReq.BlobDigests, req.Digest)
		}
		responseChansByHash[hash] = append(responseChansByHash[hash], req.ResponseChan)
	}
	res, err := f.client.FindMissingBlobs(f.ctx, batchReq)
	if err != nil {
		for _, chans := range responseChansByHash {
			for _, ch := range chans {
				ch <- findMissingResponse{Error: err}
			}
		}
		return err
	}
	missing := map[string]bool{}
	for _, d := range res.GetMissingBlobDigests() {
		missing[d.GetHash()] = true
	}
	for hash, chans := range responseChansByHash {
		for _, ch := range chans {
			ch <- findMissingResponse{Missing: missing[hash]}
		}
	}
	return nil
}

type localCache struct {
	env      *real_environment.RealEnv
	shutdown chan struct{}
}

// Runs a local cache, where all data is written to the "cache" subdirectory of
// the given bundle directory.
func startCacheServerForBundle(bundleDir string) (*localCache, error) {
	if err := os.MkdirAll(bundleDir, 0755); err != nil {
		return nil, fmt.Errorf("create target dir %q: %w", bundleDir, err)
	}
	cacheRoot := filepath.Join(bundleDir, "cache")
	hc := healthcheck.NewHealthChecker("replay_action")
	env := real_environment.NewRealEnv(hc)
	for name, value := range map[string]any{
		"app.client_identity.key":     localCacheClientIdentityKey,
		"app.client_identity.client":  "replay_action",
		"app.client_identity.origin":  "localhost",
		"cache.max_size_bytes":        500e9,
		"cache.pebble.name":           "replay_action_bundle",
		"cache.pebble.root_directory": cacheRoot,
	} {
		if err := flagutil.SetValueForFlagName(name, value, nil, false); err != nil {
			return nil, fmt.Errorf("set %s: %w", name, err)
		}
	}
	if err := pebble_cache.Register(env); err != nil {
		return nil, fmt.Errorf("register pebble cache: %w", err)
	}
	if err := clientidentity.Register(env); err != nil {
		return nil, fmt.Errorf("register client identity: %w", err)
	}
	na := nullauth.NewNullAuthenticator(true /*=anonEnabled*/)
	env.SetAuthenticator(na)
	hit_tracker.Register(env)

	// Start gRPC server
	s, err := grpc_server.New(env, grpc_server.GRPCPort(), false /*=ssl*/, grpc_server.GRPCServerConfig{})
	if err != nil {
		return nil, fmt.Errorf("new grpc server: %w", err)
	}
	grpcServer := s.GetServer()
	env.SetGRPCServer(grpcServer)
	if err := byte_stream_server.Register(env); err != nil {
		return nil, fmt.Errorf("register byte stream server: %w", err)
	}
	if err := content_addressable_storage_server.Register(env); err != nil {
		return nil, fmt.Errorf("register content addressable storage server: %w", err)
	}
	if err := action_cache_server.Register(env); err != nil {
		return nil, fmt.Errorf("register action cache server: %w", err)
	}
	if err := capabilities_server.Register(env); err != nil {
		return nil, fmt.Errorf("register capabilities server: %w", err)
	}
	bspb.RegisterByteStreamServer(grpcServer, env.GetByteStreamServer())
	repb.RegisterContentAddressableStorageServer(grpcServer, env.GetCASServer())
	repb.RegisterActionCacheServer(grpcServer, env.GetActionCacheServer())
	repb.RegisterCapabilitiesServer(grpcServer, env.GetCapabilitiesServer())
	if err = s.Start(); err != nil {
		return nil, fmt.Errorf("start grpc server: %w", err)
	}

	shutdown := make(chan struct{})
	go func() {
		defer close(shutdown)
		env.GetHealthChecker().WaitForGracefulShutdown()
	}()

	return &localCache{
		env:      env,
		shutdown: shutdown,
	}, nil
}

func (s *localCache) GRPCTarget() string {
	return fmt.Sprintf("grpc://localhost:%d", grpc_server.GRPCPort())
}

func (s *localCache) Shutdown() error {
	s.env.GetHealthChecker().Shutdown()
	<-s.shutdown
	return nil
}

// BundleManifest holds information about the actions that can be replayed using
// the bundled data.
type BundleManifest struct {
	// Executions are the executions to be replayed.
	Executions []Execution `json:"executions"`
}

// TODO: make this a proto - this is starting to contain duplicate info from
// the execution protos.
type Execution struct {
	ActionResourceName string `json:"action_resource_name"`

	QueuedTimestamp          time.Time `json:"queued_timestamp"`
	WorkerCompletedTimestamp time.Time `json:"worker_completed_timestamp"`

	TargetLabel    string `json:"target_label"`
	ActionMnemonic string `json:"action_mnemonic"`
}

type toolInvocationStream struct {
	*build_event_publisher.Publisher
	stopProgressStreaming func()
}

func startToolInvocation(ctx context.Context) (*toolInvocationStream, error) {
	// For now, just use target_executor as the BES backend.
	pub, err := build_event_publisher.New(*targetExecutor, *targetAPIKey, *toolInvocationID)
	if err != nil {
		return nil, fmt.Errorf("create build event publisher: %w", err)
	}
	pub.Start(ctx)

	stopProgress, err := connectStderrToStream(pub)
	if err != nil {
		return nil, fmt.Errorf("stream stderr: %w", err)
	}

	log.Infof("Streaming tool invocation results to %s", getToolInvocationURL())

	// Publish initial events
	var opts []string
	if *targetAPIKey != "" {
		opts = append(opts, "--remote_header='x-buildbuddy-api-key="+*targetAPIKey+"'")
	}
	if *targetExecutor != "" {
		opts = append(opts, "--remote_executor='"+*targetExecutor+"'")
	}
	started := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_Started{Started: &bespb.BuildEventId_BuildStartedId{}}},
		Children: []*bespb.BuildEventId{
			{Id: &bespb.BuildEventId_StructuredCommandLine{StructuredCommandLine: &bespb.BuildEventId_StructuredCommandLineId{CommandLineLabel: "original"}}},
			{Id: &bespb.BuildEventId_WorkspaceStatus{WorkspaceStatus: &bespb.BuildEventId_WorkspaceStatusId{}}},
			{Id: &bespb.BuildEventId_BuildMetadata{BuildMetadata: &bespb.BuildEventId_BuildMetadataId{}}},
			{Id: &bespb.BuildEventId_BuildFinished{BuildFinished: &bespb.BuildEventId_BuildFinishedId{}}},
			// TODO: would be neat to stream tool logs to the UI
			// {Id: &bespb.BuildEventId_Progress{Progress: &bespb.BuildEventId_ProgressId{OpaqueCount: 0}}},
		},
		Payload: &bespb.BuildEvent_Started{Started: &bespb.BuildStarted{
			Uuid:               *toolInvocationID,
			StartTime:          tspb.New(time.Now()),
			Command:            "replay",
			OptionsDescription: strings.Join(opts, " "),
		}},
	}
	structuredCommandLine := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_StructuredCommandLine{StructuredCommandLine: &bespb.BuildEventId_StructuredCommandLineId{CommandLineLabel: "original"}}},
		Payload: &bespb.BuildEvent_StructuredCommandLine{StructuredCommandLine: &clpb.CommandLine{
			CommandLineLabel: "canonical",
			Sections: []*clpb.CommandLineSection{
				{SectionLabel: "command options", SectionType: &clpb.CommandLineSection_OptionList{OptionList: &clpb.OptionList{Option: []*clpb.Option{
					{OptionName: "remote_executor", OptionValue: *targetExecutor},
					{OptionName: "remote_header", OptionValue: "x-buildbuddy-api-key=" + *targetAPIKey},
				}}}},
			},
		}},
	}
	workspaceStatus := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_WorkspaceStatus{WorkspaceStatus: &bespb.BuildEventId_WorkspaceStatusId{}}},
		Payload: &bespb.BuildEvent_WorkspaceStatus{WorkspaceStatus: &bespb.WorkspaceStatus{
			Item: []*bespb.WorkspaceStatus_Item{
				{Key: "BUILD_USER", Value: os.Getenv("USER")},
				{Key: "BUILD_HOST", Value: os.Getenv("HOSTNAME")},
			},
		}},
	}
	buildMetadata := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_BuildMetadata{BuildMetadata: &bespb.BuildEventId_BuildMetadataId{}}},
		Payload: &bespb.BuildEvent_BuildMetadata{BuildMetadata: &bespb.BuildMetadata{
			Metadata: map[string]string{
				"DISABLE_COMMIT_STATUS_REPORTING": "true",
			},
		}},
	}
	for _, event := range []*bespb.BuildEvent{
		started,
		structuredCommandLine,
		workspaceStatus,
		buildMetadata,
	} {
		if err := pub.Publish(event); err != nil {
			return nil, fmt.Errorf("publish build tool event: %w", err)
		}
	}

	return &toolInvocationStream{
		Publisher:             pub,
		stopProgressStreaming: stopProgress,
	}, nil
}

func setStderr(w *os.File) *os.File {
	originalStderr := os.Stderr
	os.Stderr = w
	// Reconfigure logging so it picks up the new os.Stderr. Ignore error - for
	// now - since we configured successfully before, it's unlikely this will
	// fail.
	_ = log.Configure()
	return originalStderr
}

// TODO(bduffany): share this logic with the ci_runner
func connectStderrToStream(pub *build_event_publisher.Publisher) (stop func(), err error) {
	pr, pw, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	originalStderr := setStderr(pw)

	lines := make(chan string, 1024)
	doneScanning := make(chan struct{})
	go func() {
		defer close(doneScanning)
		defer pr.Close()
		s := bufio.NewScanner(io.TeeReader(pr, originalStderr))
		s.Buffer(make([]byte, 0, 1024*1024), math.MaxInt)
		s.Split(ioutil.PreserveNewlinesSplitFunc)
		for s.Scan() {
			lines <- s.Text()
		}
	}()
	doneFlushing := make(chan struct{})
	go func() {
		defer close(doneFlushing)
		var flushTimeout <-chan time.Time
		var buf strings.Builder
		for done := false; !done; {
			select {
			case <-doneScanning:
				done = true
				// Proceed to flush
			case <-flushTimeout:
				flushTimeout = nil
				// Proceed to flush
			case line := <-lines:
				// Start flush timer
				if flushTimeout == nil {
					flushTimeout = time.After(100 * time.Millisecond)
				}
				buf.WriteString(line)
				// Proceed to flush only if the string is getting long;
				// otherwise wait until the timeout
				if buf.Len() < 16*1024 {
					continue
				}
			}
			if buf.Len() == 0 {
				continue
			}
			// Flush
			pub.Publish(&bespb.BuildEvent{
				Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_Progress{Progress: &bespb.BuildEventId_ProgressId{}}},
				Payload: &bespb.BuildEvent_Progress{Progress: &bespb.Progress{
					Stderr: buf.String(),
				}},
			})
			buf.Reset()
		}
	}()
	return func() {
		_ = pw.Close()
		<-doneScanning
		setStderr(originalStderr)
		<-doneFlushing
	}, nil
}

func (pub *toolInvocationStream) FinishWithError(toolErr error) error {
	// Disconnect stderr from the progress stream, flushing any remaining
	// progress
	pub.stopProgressStreaming()

	exitCode := &bespb.BuildFinished_ExitCode{Name: "OK", Code: 0}
	if toolErr != nil {
		exitCode = &bespb.BuildFinished_ExitCode{Name: "ERROR", Code: 1}
	}
	finished := &bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_BuildFinished{BuildFinished: &bespb.BuildEventId_BuildFinishedId{}}},
		Payload: &bespb.BuildEvent_Finished{Finished: &bespb.BuildFinished{
			ExitCode:   exitCode,
			FinishTime: tspb.New(time.Now()),
		}},
	}
	if err := pub.Publish(finished); err != nil {
		return fmt.Errorf("publish build tool event: %w", err)
	}
	if err := pub.Finish(); err != nil {
		return fmt.Errorf("finish build tool event stream: %w", err)
	}
	log.Infof("Tool invocation results available at %s", getToolInvocationURL())
	return nil
}

func getToolInvocationURL() string {
	// TODO: explicit flag
	if strings.HasPrefix(*targetExecutor, "grpc://localhost:") {
		return "http://localhost:8080/invocation/" + *toolInvocationID
	} else if strings.Contains(*targetExecutor, ".buildbuddy.dev") {
		return "https://app.buildbuddy.dev/invocation/" + *toolInvocationID
	} else {
		return "https://app.buildbuddy.io/invocation/" + *toolInvocationID
	}
}
