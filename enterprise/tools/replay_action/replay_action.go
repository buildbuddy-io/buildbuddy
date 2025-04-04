package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
)

var (
	// You probably will want to set these.
	sourceExecutor           = flag.String("source_executor", "remote.buildbuddy.io", "The backend to replay an action against.")
	targetExecutor           = flag.String("target_executor", "remote.buildbuddy.dev", "The backend to replay an action against.")
	sourceAPIKey             = flag.String("source_api_key", "", "The API key of the account that owns the action.")
	targetAPIKey             = flag.String("target_api_key", "", "API key to use for the target executor.")
	targetRemoteInstanceName = flag.String("target_remote_instance_name", "", "The remote instance name used in the source action")

	// Set one of execution_id or action_digest + source_remote_instance_name.
	executionIDs = flag.Slice("execution_id", []string{}, "Execution IDs to replay. Can be specified more than once.")
	invocationID = flag.String("invocation_id", "", "Invocation ID to replay all actions from.")

	actionDigest             = flag.String("action_digest", "", "The digest of the action you want to replay.")
	sourceRemoteInstanceName = flag.String("source_remote_instance_name", "", "The remote instance name used in the source action")

	// Less common options below.
	overrideCommand = flag.String("override_command", "", "If set, run this script (with 'sh -c') instead of the original action command line. All other properties such as environment variables and platform properties will be preserved from the original command.")
	targetHeaders   = flag.Slice("target_headers", []string{}, "A list of headers to set (format: 'key=val'")
	skipCopyFiles   = flag.Bool("skip_copy_files", false, "Skip copying files to the target. May be useful for speeding up replays after running the script at least once.")
	n               = flag.Int("n", 1, "Number of times to replay each execution. By default they'll be replayed in serial. Set --jobs to 2 or higher to run concurrently.")
	jobs            = flag.Int("jobs", 1, "Max number of concurrent jobs that can execute actions at once.")
	outDir          = flag.String("out_dir", "", "Dir for writing results and artifacts for each action. If unset, a new temp directory is created and outputs are written there.")
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
	log.Infof("Copied %s", digest.String(sourceRN.GetDigest()))
	return nil
}

func (r *Replayer) copyTree(ctx context.Context, sourceRemoteInstanceName string, tree *repb.Tree, digestType repb.DigestFunction_Value) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(100)
	srcCtx := contextWithSourceAPIKey(ctx)
	targetCtx := contextWithTargetAPIKey(ctx)
	copyDir := func(dir *repb.Directory) {
		eg.Go(func() error {
			_, err := cachetools.UploadProto(targetCtx, r.destBSClient, *targetRemoteInstanceName, digestType, dir)
			return err
		})
		for _, file := range dir.GetFiles() {
			if _, alreadyStarted := r.uploadsStarted.LoadOrStore(file.GetDigest().GetHash(), struct{}{}); alreadyStarted {
				continue
			}
			eg.Go(func() error {
				rn := digest.NewCASResourceName(file.GetDigest(), sourceRemoteInstanceName, digestType)
				err := copyFile(srcCtx, targetCtx, r.fmb, r.destBSClient, r.sourceBSClient, rn)
				if err != nil {
					return err
				}
				return nil
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
	if buf.String() != "" && *jobs == 1 {
		log.Infof("%s:\n%s", name, buf.String())
	}

	if err := os.WriteFile(filepath.Join(runDir, name), buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("write file: %w", err)
	}

	return nil
}

func getClients(target string) (bspb.ByteStreamClient, repb.ExecutionClient, repb.ContentAddressableStorageClient) {
	conn, err := grpc_client.DialSimple(target)
	if err != nil {
		log.Fatalf("Error dialing executor: %s", err.Error())
	}
	return bspb.NewByteStreamClient(conn), repb.NewExecutionClient(conn), repb.NewContentAddressableStorageClient(conn)
}

func inCopyMode(sourceRemoteInstanceName string) bool {
	return (*targetExecutor != "" && *targetExecutor != *sourceExecutor) ||
		*targetRemoteInstanceName != sourceRemoteInstanceName ||
		*targetAPIKey != *sourceAPIKey
}

func contextWithSourceAPIKey(ctx context.Context) context.Context {
	if *sourceAPIKey != "" {
		return metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *sourceAPIKey)
	}
	return ctx
}

func contextWithTargetAPIKey(ctx context.Context) context.Context {
	if *targetAPIKey != "" {
		return metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *targetAPIKey)
	}
	return ctx
}

func main() {
	flag.Parse()
	if err := log.Configure(); err != nil {
		log.Fatalf("Failed to configure logging: %s", err)
	}

	rootCtx := context.Background()

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

	srcCtx := contextWithSourceAPIKey(rootCtx)
	targetCtx := contextWithTargetAPIKey(rootCtx)

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

	log.Infof("Connecting to source %q", *sourceExecutor)
	sourceBSClient, _, sourceCASClient := getClients(*sourceExecutor)
	log.Infof("Connecting to target %q", *targetExecutor)
	destBSClient, execClient, destCASClient := getClients(*targetExecutor)

	replayer := &Replayer{
		sourceBSClient:  sourceBSClient,
		sourceCASClient: sourceCASClient,
		destBSClient:    destBSClient,
		destCASClient:   destCASClient,
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
		rn, err := digest.ParseDownloadResourceName(executionID)
		if err != nil {
			log.Fatalf("Invalid execution ID %q: %s", executionID, err)
		}
		resourceNames = append(resourceNames, rn)
	}

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
}

type Replayer struct {
	fmb *FindMissingBatcher

	replayGroup  errgroup.Group
	uploadGroup  errgroup.Group
	executeGroup errgroup.Group

	sourceBSClient, destBSClient   bspb.ByteStreamClient
	sourceCASClient, destCASClient repb.ContentAddressableStorageClient
	execClient                     repb.ExecutionClient

	// Digests that have already started uploading and do not need to be
	// re-uploaded. Keys are digest hashes; values are arbitrary.
	uploadsStarted sync.Map
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

	return r.execute(ctx, srcCtx, targetCtx, action, sourceExecutionRN)
}

func (r *Replayer) upload(ctx, srcCtx, targetCtx context.Context, action *repb.Action, sourceExecutionRN *digest.CASResourceName) error {
	s := sourceExecutionRN.DownloadString()
	log.Infof("Uploading Action, Command, and inputs for execution %q", s)
	eg, targetCtx := errgroup.WithContext(targetCtx)
	eg.Go(func() error {
		actionRN := sourceExecutionRN
		if err := copyFile(srcCtx, targetCtx, r.fmb, r.destBSClient, r.sourceBSClient, actionRN); err != nil {
			return status.WrapError(err, "copy action")
		}
		return nil
	})
	eg.Go(func() error {
		commandRN := digest.NewCASResourceName(action.GetCommandDigest(), sourceExecutionRN.GetInstanceName(), sourceExecutionRN.GetDigestFunction())
		if err := copyFile(srcCtx, targetCtx, r.fmb, r.destBSClient, r.sourceBSClient, commandRN); err != nil {
			return status.WrapError(err, "copy command")
		}
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
		return nil
	})
	if err := eg.Wait(); err != nil {
		return err
	}
	log.Infof("Finished copying files for execution %s", s)
	return nil
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
			log.CtxInfof(ctx, "ExecuteResponse: %s", string(jb))
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
