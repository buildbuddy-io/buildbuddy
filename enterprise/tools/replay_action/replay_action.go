package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pebble_cache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/clientidentity"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ocicache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ocimanifest"
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
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/shlex"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/jonboulle/clockwork"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
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

	actionDigest             = flag.String("action_digest", "", "The digest of the action you want to replay.")
	sourceRemoteInstanceName = flag.String("source_remote_instance_name", "", "The remote instance name used in the source action")

	showOutput = flag.Bool("show_output", true, "Show stdout/stderr from the action.")

	// Less common options below.
	overrideCommand = flag.String("override_command", "", "If set, run this script (with 'sh -c') instead of the original action command line. All other properties such as environment variables and platform properties will be preserved from the original command. The original command can be referenced via the $ORIGINAL_COMMAND environment variable.")
	targetHeaders   = flag.Slice("target_headers", []string{}, "A list of headers to set (format: 'key=val'")
	skipCopyFiles   = flag.Bool("skip_copy_files", false, "Skip copying files to the target. May be useful for speeding up replays after running the script at least once.")
	n               = flag.Int("n", 1, "Number of times to replay each execution. By default they'll be replayed in serial. Set --jobs to 2 or higher to run concurrently.")
	jobs            = flag.Int("jobs", 1, "Max number of concurrent jobs that can execute actions at once.")
	outDir          = flag.String("out_dir", "", "Dir for writing results and artifacts for each action. If unset, a new temp directory is created and outputs are written there.")
)

const (
	localCacheClientIdentityKey = "localhost"
	bundleManifestFileName      = "bundle_manifest.json"
)

// Example usage:
// $ bazel run //enterprise/tools/replay_action:replay_action -- \
//   --source_executor="grpcs://remote.buildbuddy.io" \
//   --action_digest="blake3/f31e59431cdc5d631853e28151fb664f859b5f4c5dc94f0695408a6d31b84724/142"
//

func diffTimeProtos(start, end *tspb.Timestamp) time.Duration {
	return end.AsTime().Sub(start.AsTime())
}

func logExecutionMetadata(ctx context.Context, md *repb.ExecutedActionMetadata) {
	qTime := diffTimeProtos(md.GetQueuedTimestamp(), md.GetWorkerStartTimestamp())
	fetchTime := diffTimeProtos(md.GetInputFetchStartTimestamp(), md.GetInputFetchCompletedTimestamp())
	execTime := diffTimeProtos(md.GetExecutionStartTimestamp(), md.GetExecutionCompletedTimestamp())
	uploadTime := diffTimeProtos(md.GetOutputUploadStartTimestamp(), md.GetOutputUploadCompletedTimestamp())
	cpuMillis := md.GetUsageStats().GetCpuNanos() / 1e6
	log.CtxInfof(ctx, "Completed [queue: %4dms, fetch: %4dms, exec: %4dms, upload: %4dms, cpu_total: %4dms]",
		qTime.Milliseconds(), fetchTime.Milliseconds(), execTime.Milliseconds(), uploadTime.Milliseconds(), cpuMillis)
}

func copyFile(srcCtx, targetCtx context.Context, fmb *FindMissingBatcher, to, from bspb.ByteStreamClient, sourceRN *digest.CASResourceName) error {
	targetRN := digest.NewCASResourceName(sourceRN.GetDigest(), *targetRemoteInstanceName, sourceRN.GetDigestFunction())
	exists, err := fmb.Exists(targetCtx, targetRN.GetDigest())
	if err != nil {
		return err
	}
	if exists {
		log.Debugf("Copy %s: already exists", digest.String(targetRN.GetDigest()))
		return nil
	}
	buf := &bytes.Buffer{}
	if err := cachetools.GetBlob(srcCtx, from, sourceRN, buf); err != nil {
		return err
	}
	seekBuf := bytes.NewReader(buf.Bytes())
	d2, _, err := cachetools.UploadFromReader(targetCtx, to, targetRN, seekBuf)
	if err != nil {
		return err
	}
	if d2.GetHash() != sourceRN.GetDigest().GetHash() || d2.GetSizeBytes() != sourceRN.GetDigest().GetSizeBytes() {
		return status.FailedPreconditionErrorf("copyFile mismatch: %s != %s", digest.String(d2), digest.String(sourceRN.GetDigest()))
	}
	log.Infof("Copied input %s", digest.String(sourceRN.GetDigest()))
	return nil
}

func (r *Replayer) copyTree(ctx context.Context, sourceRemoteInstanceName string, tree *repb.Tree, digestType repb.DigestFunction_Value) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(100)
	srcCtx := contextWithSourceCredentials(ctx)
	targetCtx := contextWithTargetCredentials(ctx)
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

func getClients(target string) (io.Closer, bspb.ByteStreamClient, repb.ExecutionClient, repb.ContentAddressableStorageClient, repb.ActionCacheClient) {
	conn, err := grpc_client.DialSimple(target)
	if err != nil {
		log.Fatalf("Error dialing executor: %s", err.Error())
	}
	bs := bspb.NewByteStreamClient(conn)
	exec := repb.NewExecutionClient(conn)
	cas := repb.NewContentAddressableStorageClient(conn)
	ac := repb.NewActionCacheClient(conn)
	return conn, bs, exec, cas, ac
}

func inCopyMode(sourceRemoteInstanceName string) bool {
	return (*targetExecutor != "" && *targetExecutor != *sourceExecutor) ||
		*targetRemoteInstanceName != sourceRemoteInstanceName ||
		*targetAPIKey != *sourceAPIKey
}

func contextWithSourceCredentials(ctx context.Context) context.Context {
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
			log.Fatalf("Failed to set source client identity: %s", err)
		}
	}
	return ctx
}

func contextWithTargetCredentials(ctx context.Context) context.Context {
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
			log.Fatalf("Failed to set target client identity: %s", err)
		}
	}
	return ctx
}

func main() {
	flag.Parse()
	if err := log.Configure(); err != nil {
		log.Fatalf("Failed to configure logging: %s", err)
	}

	rootCtx := context.Background()

	if *targetBundle != "" && (*targetExecutor != "" || *targetAPIKey != "" || *targetClientIdentityKey != "") {
		log.Fatalf("Cannot set both target_bundle and other target_* flags")
	}
	if *targetBundle == "" && *targetExecutor == "" {
		log.Fatalf("Must set either target_bundle or target_executor")
	}
	if *sourceBundle != "" && (*sourceExecutor != "" || *sourceAPIKey != "" || *sourceClientIdentityKey != "") {
		log.Fatalf("Cannot set both source_bundle and other source_* flags")
	}
	if *sourceBundle == "" && *sourceExecutor == "" {
		log.Fatalf("Must set either source_bundle or source_executor")
	}
	if *sourceBundle != "" && *targetBundle != "" {
		log.Fatalf("Cannot set both source_bundle and target_bundle (use 'cp -r' to copy bundles)")
	}

	if *sourceAPIKey != "" && *targetAPIKey == "" {
		log.Warningf("--target_api_key is not set, but --source_api_key was set. Replaying as anonymous user.")
	}

	if *outDir == "" {
		tmp, err := os.MkdirTemp("", "replay-action-*")
		if err != nil {
			log.Fatalf("Failed to create output dir: %s", err)
		}
		log.Infof("Writing results to %s", tmp)
		*outDir = tmp
	}

	srcCtx := contextWithSourceCredentials(rootCtx)
	targetCtx := contextWithTargetCredentials(rootCtx)

	headersToSet := make([]string, 0)
	for _, targetHeader := range *targetHeaders {
		pair := strings.SplitN(targetHeader, "=", 2)
		if len(pair) != 2 {
			log.Fatalf("Target headers must be of form key=val, got: %q", targetHeader)
		}
		headersToSet = append(headersToSet, pair[0])
		headersToSet = append(headersToSet, pair[1])
	}
	if len(headersToSet) > 0 {
		targetCtx = metadata.AppendToOutgoingContext(targetCtx, headersToSet...)
	}

	if *sourceBundle != "" {
		localCache, err := startCacheServerForBundle(*sourceBundle)
		if err != nil {
			log.Fatalf("Failed to start local cache: %s", err)
		}
		defer localCache.Shutdown()
		*sourceExecutor = localCache.GRPCTarget()
	}
	log.Infof("Connecting to source %q", *sourceExecutor)
	sourceConn, sourceBSClient, _, sourceCASClient, sourceACClient := getClients(*sourceExecutor)
	defer sourceConn.Close()

	if *targetBundle != "" {
		localCache, err := startCacheServerForBundle(*targetBundle)
		if err != nil {
			log.Fatalf("Failed to start local cache: %s", err)
		}
		defer localCache.Shutdown()
		*targetExecutor = localCache.GRPCTarget()
	}
	log.Infof("Connecting to target %q", *targetExecutor)
	destConn, destBSClient, execClient, destCASClient, destACClient := getClients(*targetExecutor)
	defer destConn.Close()

	replayer := &Replayer{
		sourceBSClient:  sourceBSClient,
		sourceCASClient: sourceCASClient,
		sourceACClient:  sourceACClient,
		destBSClient:    destBSClient,
		destCASClient:   destCASClient,
		destACClient:    destACClient,
		execClient:      execClient,
	}
	replayer.uploadGroup.SetLimit(3)
	replayer.executeGroup.SetLimit(*jobs)

	var resourceNames []*digest.CASResourceName

	if *actionDigest != "" {
		// For backwards compatibility, attempt to fixup old style digest
		// strings that don't start with a '/blobs/' prefix.
		digestString := *actionDigest
		if !strings.HasPrefix(digestString, "/blobs") {
			digestString = *sourceRemoteInstanceName + "/blobs/" + digestString
		}
		rn, err := digest.ParseDownloadResourceName(digestString)
		if err != nil {
			log.Fatalf("Error parsing action digest %q: %s", *actionDigest, err)
		}
		resourceNames = append(resourceNames, rn)
	}
	for _, executionID := range *executionIDs {
		rn, err := digest.ParseUploadResourceName(executionID)
		if err != nil {
			log.Fatalf("Invalid execution ID %q: %s", executionID, err)
		}
		resourceNames = append(resourceNames, rn)
	}
	// If replaying from a bundle, and not filtering to specific execution(s)
	// from the bundle, load execution resource names from the manifest.
	if *sourceBundle != "" && len(resourceNames) == 0 {
		bundleManifestBytes, err := os.ReadFile(filepath.Join(*sourceBundle, bundleManifestFileName))
		if err != nil {
			log.Fatalf("Failed to read bundle manifest: %s", err)
		}
		bundleManifest := &BundleManifest{}
		if err := json.Unmarshal(bundleManifestBytes, bundleManifest); err != nil {
			log.Fatalf("Failed to unmarshal bundle manifest: %s", err)
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
		for _, execution := range bundleManifest.Executions {
			rn, err := digest.ParseDownloadResourceName(execution.ActionResourceName)
			if err != nil {
				log.Fatalf("Invalid action resource name %q: %s", execution.ActionResourceName, err)
			}
			resourceNames = append(resourceNames, rn)
		}
	}

	// Execution metadata (for bundle manifest) by resource name.
	// Only populated if replaying from an invocation.
	executionMetadata := map[*digest.CASResourceName]Execution{}

	if *invocationID != "" {
		conn, err := grpc_client.DialSimple(*sourceExecutor)
		if err != nil {
			log.Fatalf("Error dialing executor: %s", err.Error())
		}
		client := bbspb.NewBuildBuddyServiceClient(conn)
		rsp, err := client.GetExecution(srcCtx, &espb.GetExecutionRequest{
			ExecutionLookup: &espb.ExecutionLookup{
				InvocationId: *invocationID,
			},
		})
		if err != nil {
			log.Fatalf("Could not retrieve invocation executions: %s", err)
		}
		for _, e := range rsp.Execution {
			rn, err := digest.ParseUploadResourceName(e.ExecutionId)
			if err != nil {
				log.Fatalf("Invalid execution ID %q: %s", e.ExecutionId, err)
			}
			resourceNames = append(resourceNames, rn)
			executionMetadata[rn] = Execution{
				ActionResourceName:       rn.DownloadString(),
				QueuedTimestamp:          e.ExecutedActionMetadata.QueuedTimestamp.AsTime(),
				WorkerCompletedTimestamp: e.ExecutedActionMetadata.WorkerCompletedTimestamp.AsTime(),
			}
		}
	}

	if len(resourceNames) == 0 {
		log.Fatalf("Missing -action_digest or -execution_id or -invocation_id")
	}

	defer func() {
		if err := replayer.Wait(); err != nil {
			log.Fatalf("Replay failed: %s", err)
		}
	}()
	for _, rn := range resourceNames {
		if err := replayer.Start(rootCtx, srcCtx, targetCtx, rn); err != nil {
			log.Errorf("Failed to start replay: %s", err)
			return
		}
	}
	if *targetBundle != "" {
		bundleManifest := &BundleManifest{
			Executions: make([]Execution, 0, len(resourceNames)),
		}
		for _, rn := range resourceNames {
			if md, ok := executionMetadata[rn]; ok {
				bundleManifest.Executions = append(bundleManifest.Executions, md)
			} else {
				bundleManifest.Executions = append(bundleManifest.Executions, Execution{
					ActionResourceName: rn.DownloadString(),
				})
			}
		}
		bundleManifestBytes, err := json.MarshalIndent(bundleManifest, "", "  ")
		if err != nil {
			log.Fatalf("Failed to marshal bundle manifest: %s", err)
		}
		if err := os.WriteFile(filepath.Join(*targetBundle, bundleManifestFileName), bundleManifestBytes, 0644); err != nil {
			log.Fatalf("Failed to write bundle manifest: %s", err)
		}
	}
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
}

func (r *Replayer) Start(ctx, srcCtx, targetCtx context.Context, sourceExecutionRN *digest.CASResourceName) error {
	if r.fmb == nil {
		r.fmb = NewFindMissingBatcher(targetCtx, *targetRemoteInstanceName, sourceExecutionRN.GetDigestFunction(), r.destCASClient, FindMissingBatcherOpts{})
	}

	r.replayGroup.Go(func() error {
		return r.replay(ctx, srcCtx, targetCtx, sourceExecutionRN)
	})
	return nil
}

func (r *Replayer) Wait() error {
	return r.replayGroup.Wait()
}

func (r *Replayer) replay(ctx, srcCtx, targetCtx context.Context, sourceExecutionRN *digest.CASResourceName) error {
	// Fetch the action to ensure it exists.
	action := &repb.Action{}
	if err := cachetools.GetBlobAsProto(srcCtx, r.sourceBSClient, sourceExecutionRN, action); err != nil {
		return status.WrapError(err, "fetch action")
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

	return r.execute(ctx, srcCtx, targetCtx, action, sourceExecutionRN)
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
		if err := r.copyTree(ctx, sourceExecutionRN.GetInstanceName(), tree, sourceExecutionRN.GetDigestFunction()); err != nil {
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

func (r *Replayer) execute(ctx, srcCtx, targetCtx context.Context, action *repb.Action, sourceExecutionRN *digest.CASResourceName) error {
	// If we're overriding the command, do that now.
	if *overrideCommand != "" {
		// Download the command and update arguments.
		sourceCRN := digest.NewCASResourceName(action.GetCommandDigest(), sourceExecutionRN.GetInstanceName(), sourceExecutionRN.GetDigestFunction())
		cmd := &repb.Command{}
		if err := cachetools.GetBlobAsProto(srcCtx, r.sourceBSClient, sourceCRN, cmd); err != nil {
			log.Fatalf("Failed to get command: %s", err)
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
			log.Fatalf("Failed to upload new command: %s", err)
		}
		action = action.CloneVT()
		action.CommandDigest = cd
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
			err := execute(targetCtx, r.execClient, r.destBSClient, i, sourceExecutionRN, execReq)
			executeErr <- err
			return err
		})
	}
	var lastErr error
	for i := 1; i <= *n; i++ {
		if err := <-executeErr; err != nil {
			log.CtxWarningf(ctx, "Execute failed: %s", err)
			lastErr = err
		}
	}
	return lastErr
}

func execute(ctx context.Context, execClient repb.ExecutionClient, bsClient bspb.ByteStreamClient, i int, sourceExecutionID *digest.CASResourceName, req *repb.ExecuteRequest) error {
	ctx = log.EnrichContext(ctx, "run", fmt.Sprintf("%d/%d", i, *n))

	actionId := sourceExecutionID.GetDigest().GetHash()
	iid := uuid.New()
	rmd := &repb.RequestMetadata{ActionId: actionId, ToolInvocationId: iid}
	ctx, err := bazel_request.WithRequestMetadata(ctx, rmd)
	if err != nil {
		return status.WrapError(err, "set request metadata")
	}
	log.CtxInfof(ctx, "Starting, invocation id %q", iid)
	stream, err := execClient.Execute(ctx, req)
	if err != nil {
		return status.WrapError(err, "Execute")
	}
	printedExecutionID := false
	runDir := filepath.Join(*outDir, fmt.Sprintf("%s/%d", actionId, i))
	if err := os.MkdirAll(runDir, 0755); err != nil {
		log.Fatalf("Failed to make run dir %q: %s", runDir, err)
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
			logExecutionMetadata(ctx, response.GetResult().GetExecutionMetadata())
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

type Execution struct {
	ActionResourceName string `json:"action_resource_name"`

	QueuedTimestamp          time.Time `json:"queued_timestamp"`
	WorkerCompletedTimestamp time.Time `json:"worker_completed_timestamp"`
}
