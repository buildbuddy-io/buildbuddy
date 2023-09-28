package main

import (
	"bytes"
	"context"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
)

var (
	// You probably will want to set these.
	actionDigest             = flag.String("action_digest", "", "The digest of the action you want to replay.")
	sourceExecutor           = flag.String("source_executor", "grpcs://remote.buildbuddy.dev", "The backend to replay an action against.")
	targetExecutor           = flag.String("target_executor", "", "The backend to replay an action against.")
	sourceAPIKey             = flag.String("source_api_key", "", "The API key of the account that owns the action.")
	targetAPIKey             = flag.String("target_api_key", "", "API key to use for the target executor. If not set, uses the source API key.")
	sourceRemoteInstanceName = flag.String("source_remote_instance_name", "", "The remote instance name used in the source action")
	targetRemoteInstanceName = flag.String("target_remote_instance_name", "", "The remote instance name used in the source action")

	// Less common options below.
	targetHeaders = flag.Slice("target_headers", []string{}, "A list of headers to set (format: 'key=val'")
	n             = flag.Int("n", 1, "Number of times to replay the action. By default they'll be replayed in serial. Set --jobs to 2 or higher to run concurrently.")
	jobs          = flag.Int("jobs", 1, "Max number of concurrent jobs that can execute actions at once.")
)

// Example usage:
// $ blaze run tools/replay_action:replay_action -- \
//   --source_executor="grpcs://remote.buildbuddy.io" \
//   --action_digest="f31e59431cdc5d631853e28151fb664f859b5f4c5dc94f0695408a6d31b84724/142"
//

func diffTimeProtos(start, end *tspb.Timestamp) time.Duration {
	return end.AsTime().Sub(start.AsTime())
}

func logExecutionMetadata(i int, d *repb.Digest, md *repb.ExecutedActionMetadata) {
	qTime := diffTimeProtos(md.GetQueuedTimestamp(), md.GetWorkerStartTimestamp())
	fetchTime := diffTimeProtos(md.GetInputFetchStartTimestamp(), md.GetInputFetchCompletedTimestamp())
	execTime := diffTimeProtos(md.GetExecutionStartTimestamp(), md.GetExecutionCompletedTimestamp())
	uploadTime := diffTimeProtos(md.GetOutputUploadStartTimestamp(), md.GetOutputUploadCompletedTimestamp())
	cpuMillis := md.GetUsageStats().GetCpuNanos() / 1e6
	log.Infof("Completed %d of %d [queue: %04dms, fetch: %04dms, exec: %04dms, upload: %04dms, cpu: %04dm]",
		i, *n, qTime.Milliseconds(), fetchTime.Milliseconds(), execTime.Milliseconds(), uploadTime.Milliseconds(), cpuMillis)
}

func copyFile(srcCtx, targetCtx context.Context, fmb *FindMissingBatcher, to, from bspb.ByteStreamClient, d *repb.Digest) error {
	outd := digest.NewResourceName(d, *targetRemoteInstanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256)
	exists, err := fmb.Exists(targetCtx, outd.GetDigest())
	if err != nil {
		return err
	}
	if exists {
		log.Infof("Copy %s: already exists", digest.String(outd.GetDigest()))
		return nil
	}
	buf := &bytes.Buffer{}
	ind := digest.NewResourceName(d, *sourceRemoteInstanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256)
	if err := cachetools.GetBlob(srcCtx, from, ind, buf); err != nil {
		return err
	}
	seekBuf := bytes.NewReader(buf.Bytes())
	d2, err := cachetools.UploadFromReader(targetCtx, to, outd, seekBuf)
	if err != nil {
		return err
	}
	if d2.GetHash() != d.GetHash() || d2.GetSizeBytes() != d.GetSizeBytes() {
		return status.FailedPreconditionErrorf("copyFile mismatch: %s != %s", digest.String(d2), digest.String(d))
	}
	log.Infof("Copied %s", digest.String(d))
	return nil
}

func copyTree(ctx context.Context, fmb *FindMissingBatcher, to, from bspb.ByteStreamClient, tree *repb.Tree) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(100)
	srcCtx := contextWithSourceAPIKey(ctx)
	targetCtx := contextWithTargetAPIKey(ctx)
	copyDir := func(dir *repb.Directory) {
		eg.Go(func() error {
			_, err := cachetools.UploadProto(targetCtx, to, *targetRemoteInstanceName, repb.DigestFunction_SHA256, dir)
			return err
		})
		for _, file := range dir.GetFiles() {
			file := file
			eg.Go(func() error {
				return copyFile(srcCtx, targetCtx, fmb, to, from, file.GetDigest())
			})
		}
	}
	copyDir(tree.GetRoot())
	for _, dir := range tree.GetChildren() {
		copyDir(dir)
	}
	return eg.Wait()
}

func printOutputFile(ctx context.Context, from bspb.ByteStreamClient, d *repb.Digest, tag string) error {
	buf := &bytes.Buffer{}
	ind := digest.NewResourceName(d, *targetRemoteInstanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256)
	if err := cachetools.GetBlob(ctx, from, ind, buf); err != nil {
		return err
	}
	content := " <empty>"
	if buf.String() != "" {
		content = "\n" + buf.String()
	}
	log.Infof("%s:%s", tag, content)
	return nil
}

func getClients(target string) (bspb.ByteStreamClient, repb.ExecutionClient, repb.ContentAddressableStorageClient) {
	conn, err := grpc_client.DialSimple(target)
	if err != nil {
		log.Fatalf("Error dialing executor: %s", err.Error())
	}
	return bspb.NewByteStreamClient(conn), repb.NewExecutionClient(conn), repb.NewContentAddressableStorageClient(conn)
}

func inCopyMode() bool {
	return (*targetExecutor != "" && *targetExecutor != *sourceExecutor) || *targetRemoteInstanceName != *sourceRemoteInstanceName
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

	rootCtx := context.Background()

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

	log.Infof("Connecting to %q", *sourceExecutor)
	var sourceBSClient, destBSClient bspb.ByteStreamClient
	var sourceCASClient, destCASClient repb.ContentAddressableStorageClient
	var execClient repb.ExecutionClient
	sourceBSClient, execClient, sourceCASClient = getClients(*sourceExecutor)
	if inCopyMode() {
		destBSClient, execClient, destCASClient = getClients(*targetExecutor)
	}

	// For backwards compatibility, attempt to fixup old style digest
	// strings that don't start with a '/blobs/' prefix.
	digestString := *actionDigest
	if !strings.HasPrefix(digestString, "/blobs") {
		digestString = "/blobs/" + digestString
	}

	actionInstanceDigest, err := digest.ParseDownloadResourceName(digestString)
	if err != nil {
		log.Fatalf("Error parsing action digest %q: %s", *actionDigest, err)
	}

	// Fetch the action to ensure it exists.
	action := &repb.Action{}
	d := actionInstanceDigest.GetDigest()
	if err := cachetools.GetBlobAsProto(srcCtx, sourceBSClient, actionInstanceDigest, action); err != nil {
		log.Fatalf("Error fetching action: %s", err.Error())
	}
	// If remote_executor and target_executor are not the same, copy the files.
	if inCopyMode() {
		fmb := NewFindMissingBatcher(targetCtx, *targetRemoteInstanceName, destCASClient, FindMissingBatcherOpts{})
		eg, targetCtx := errgroup.WithContext(targetCtx)
		eg.Go(func() error {
			if err := copyFile(srcCtx, targetCtx, fmb, destBSClient, sourceBSClient, d); err != nil {
				return status.WrapError(err, "copy action")
			}
			return nil
		})
		eg.Go(func() error {
			if err := copyFile(srcCtx, targetCtx, fmb, destBSClient, sourceBSClient, action.GetCommandDigest()); err != nil {
				return status.WrapError(err, "copy command")
			}
			return nil
		})
		eg.Go(func() error {
			treeRN := digest.NewResourceName(action.GetInputRootDigest(), *sourceRemoteInstanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256)
			tree, err := cachetools.GetTreeFromRootDirectoryDigest(srcCtx, sourceCASClient, treeRN)
			if err != nil {
				return status.WrapError(err, "GetTree")
			}
			if err := copyTree(rootCtx, fmb, destBSClient, sourceBSClient, tree); err != nil {
				return status.WrapError(err, "copy tree")
			}
			return nil
		})
		if err := eg.Wait(); err != nil {
			log.Fatalf("Failed to copy files: %s", err)
		}
		log.Infof("Finished copying files.")
	}
	execReq := &repb.ExecuteRequest{
		InstanceName:    *targetRemoteInstanceName,
		SkipCacheLookup: true,
		ActionDigest:    d,
	}
	eg := &errgroup.Group{}
	eg.SetLimit(*jobs)
	for i := 1; i <= *n; i++ {
		i := i
		eg.Go(func() error {
			execute(targetCtx, execClient, destBSClient, i, d, execReq)
			return nil
		})
	}
	eg.Wait()
}

func execute(ctx context.Context, execClient repb.ExecutionClient, bsClient bspb.ByteStreamClient, i int, d *repb.Digest, req *repb.ExecuteRequest) {
	log.Infof("Starting action %d of %d...", i, *n)
	stream, err := execClient.Execute(ctx, req)
	if err != nil {
		log.Fatalf(err.Error())
	}
	printedExecutionID := false
	for {
		op, err := stream.Recv()
		if err != nil {
			log.Fatalf("Error on stream: %s", err.Error())
		}
		if !printedExecutionID {
			log.Infof("Started task %q", op.GetName())
			printedExecutionID = true
		}
		log.Infof("Execution stage: %s", operation.ExtractStage(op))
		if op.GetDone() {
			metadata := &repb.ExecuteOperationMetadata{}
			if err := op.GetMetadata().UnmarshalTo(metadata); err == nil {
				jb, _ := (protojson.MarshalOptions{Multiline: true}).Marshal(metadata)
				log.Infof("Metadata: %s", string(jb))
			}

			response := &repb.ExecuteResponse{}
			if err := op.GetResponse().UnmarshalTo(response); err != nil {
				log.Errorf("Failed to unmarshal response: %s", err)
				return
			}

			if err := gstatus.ErrorProto(response.GetStatus()); err != nil {
				log.Errorf("Execution failed: %s", err)
				break
			}

			jb, _ := (protojson.MarshalOptions{Multiline: true}).Marshal(response)
			log.Infof("ExecuteResponse: %s", string(jb))
			result := response.GetResult()
			if result.GetExitCode() != 0 {
				log.Warningf("Action exited with code %d", result.GetExitCode())
			}
			// Print stdout and stderr but only when running a single action.
			if *n == 1 {
				printOutputFile(ctx, bsClient, result.GetStdoutDigest(), "stdout")
				printOutputFile(ctx, bsClient, result.GetStderrDigest(), "stderr")
			}
			logExecutionMetadata(i, d, response.GetResult().GetExecutionMetadata())
			break
		}
	}
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
	ctx          context.Context
	instanceName string
	client       repb.ContentAddressableStorageClient
	opts         FindMissingBatcherOpts
	reqs         chan findMissingRequest
}

func NewFindMissingBatcher(ctx context.Context, instanceName string, client repb.ContentAddressableStorageClient, opts FindMissingBatcherOpts) *FindMissingBatcher {
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
		ctx:          ctx,
		instanceName: instanceName,
		client:       client,
		opts:         opts,
		reqs:         make(chan findMissingRequest, 128),
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
	batchReq := &repb.FindMissingBlobsRequest{}
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
