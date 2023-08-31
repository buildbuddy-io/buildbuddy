package main

import (
	"bytes"
	"context"
	"flag"
	"log"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
	bspb "google.golang.org/genproto/googleapis/bytestream"
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
	skipCopyFiles            = flag.Bool("skip_copy_files", false, "Whether to skip copying input files.")
	targetHeaders            = flagutil.New("target_headers", []string{}, "A list of headers to set (format: 'key=val'")

	// Less common options below.
	n    = flag.Int("n", 1, "Number of times to replay the action.")
	jobs = flag.Int("jobs", 1, "Number of concurrent jobs to use when replaying the action.")
)

// Example usage:
// $ blaze run tools/replay_action:replay_action -- \
//   --source_executor="grpcs://remote.buildbuddy.io" \
//   --action_digest="f31e59431cdc5d631853e28151fb664f859b5f4c5dc94f0695408a6d31b84724/142"
//

func diffTimeProtos(startPb, endPb *tspb.Timestamp) time.Duration {
	start, _ := ptypes.Timestamp(startPb)
	end, _ := ptypes.Timestamp(endPb)
	return end.Sub(start)
}

func logActionResult(d *repb.Digest, md *repb.ExecutedActionMetadata) {
	qTime := diffTimeProtos(md.GetQueuedTimestamp(), md.GetWorkerStartTimestamp())
	workTime := diffTimeProtos(md.GetWorkerStartTimestamp(), md.GetWorkerCompletedTimestamp())
	fetchTime := diffTimeProtos(md.GetInputFetchStartTimestamp(), md.GetInputFetchCompletedTimestamp())
	execTime := diffTimeProtos(md.GetExecutionStartTimestamp(), md.GetExecutionCompletedTimestamp())
	uploadTime := diffTimeProtos(md.GetOutputUploadStartTimestamp(), md.GetOutputUploadCompletedTimestamp())
	log.Printf("%q completed action '%s/%d' [q: %02dms work: %02dms, fetch: %02dms, exec: %02dms, upload: %02dms]",
		md.GetWorker(), d.GetHash(), d.GetSizeBytes(), qTime.Milliseconds(), workTime.Milliseconds(),
		fetchTime.Milliseconds(), execTime.Milliseconds(), uploadTime.Milliseconds())
}

func copyFile(srcCtx, targetCtx context.Context, to, from bspb.ByteStreamClient, d *repb.Digest) error {
	buf := &bytes.Buffer{}
	ind := digest.NewResourceName(d, *sourceRemoteInstanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256)
	if err := cachetools.GetBlob(srcCtx, from, ind, buf); err != nil {
		return err
	}
	seekBuf := bytes.NewReader(buf.Bytes())
	outd := digest.NewResourceName(d, *targetRemoteInstanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256)
	d2, err := cachetools.UploadFromReader(targetCtx, to, outd, seekBuf)
	if err != nil {
		return err
	}
	if d2.GetHash() != d.GetHash() || d2.GetSizeBytes() != d.GetSizeBytes() {
		return status.FailedPreconditionErrorf("copyFile mismatch: %s != %s", proto.MarshalTextString(d2), proto.MarshalTextString(d))
	}
	log.Printf("copied file '%s/%d'", d.GetHash(), d.GetSizeBytes())
	return nil
}

func copyFiles(rootCtx context.Context, to, from bspb.ByteStreamClient, inputRootDigest *repb.Digest) error {
	g, gCtx := errgroup.WithContext(rootCtx)
	srcCtx := contextWithSourceAPIKey(gCtx)
	targetCtx := contextWithTargetAPIKey(gCtx)

	var copyDir func(d *repb.Digest) error
	copyDir = func(d *repb.Digest) error {
		dir := repb.Directory{}
		ind := digest.NewResourceName(d, *sourceRemoteInstanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256)
		if err := cachetools.GetBlobAsProto(srcCtx, from, ind, &dir); err != nil {
			return err
		}
		for _, fn := range dir.GetFiles() {
			loopFn := fn
			g.Go(func() error {
				return copyFile(srcCtx, targetCtx, to, from, loopFn.GetDigest())
			})
		}
		for _, dn := range dir.GetDirectories() {
			loopDn := dn
			g.Go(func() error {
				return copyDir(loopDn.GetDigest())
			})
		}
		d2, err := cachetools.UploadProto(targetCtx, to, *targetRemoteInstanceName, repb.DigestFunction_SHA256, &dir)
		if err != nil {
			return err
		}
		if d2.GetHash() != d.GetHash() || d2.GetSizeBytes() != d.GetSizeBytes() {
			return status.FailedPreconditionErrorf("copyDir mismatch: %s != %s", proto.MarshalTextString(d2), proto.MarshalTextString(d))
		}
		log.Printf("copied dir '%s/%d'", d.GetHash(), d.GetSizeBytes())
		return nil
	}
	if err := copyDir(inputRootDigest); err != nil {
		return err
	}
	return g.Wait()
}

func printOutputFile(ctx context.Context, from bspb.ByteStreamClient, d *repb.Digest, tag string) error {
	buf := &bytes.Buffer{}
	ind := digest.NewResourceName(d, *targetRemoteInstanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256)
	if err := cachetools.GetBlob(ctx, from, ind, buf); err != nil {
		return err
	}
	log.Printf("%s:\n%s", tag, buf.String())
	return nil
}

func getClients(target string) (bspb.ByteStreamClient, repb.ExecutionClient) {
	conn, err := grpc_client.DialTarget(target)
	if err != nil {
		log.Fatalf("Error dialing executor: %s", err.Error())
	}
	return bspb.NewByteStreamClient(conn), repb.NewExecutionClient(conn)
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
	if *sourceAPIKey != "" {
		return metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *sourceAPIKey)
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

	log.Printf("Connecting to %q", *sourceExecutor)
	var sourceBSClient, destBSClient bspb.ByteStreamClient
	var execClient repb.ExecutionClient
	sourceBSClient, execClient = getClients(*sourceExecutor)
	if inCopyMode() {
		destBSClient, execClient = getClients(*targetExecutor)
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
	if inCopyMode() && !*skipCopyFiles {
		if err := copyFile(srcCtx, targetCtx, destBSClient, sourceBSClient, d); err != nil {
			log.Fatalf("Error copying action: %s", err.Error())
		}
		if err := copyFile(srcCtx, targetCtx, destBSClient, sourceBSClient, action.GetCommandDigest()); err != nil {
			log.Fatalf("Error copying command: %s", err.Error())
		}
		if err := copyFiles(rootCtx, destBSClient, sourceBSClient, action.GetInputRootDigest()); err != nil {
			log.Fatalf("Error copying files: %s", err.Error())
		}
		log.Printf("Finished copying files.")
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
	start := time.Now()
	log.Printf("Starting remote execution, run %d of %d...", i, *n)
	stream, err := execClient.Execute(ctx, req)
	if err != nil {
		log.Fatalf(err.Error())
	}
	for {
		op, err := stream.Recv()
		if err != nil {
			log.Fatalf("Error on stream: %s", err.Error())
		}
		// metadata := &repb.ExecuteOperationMetadata{}
		// if err := ptypes.UnmarshalAny(op.GetMetadata(), metadata); err == nil {
		// 	// log.Printf("Metadata: %s", proto.MarshalTextString(metadata))
		// }
		if op.GetDone() {
			log.Printf("Execution finished in %s", time.Since(start))
			// response := &repb.ExecuteResponse{}
			// if err := ptypes.UnmarshalAny(op.GetResponse(), response); err == nil {
			// log.Printf("Response: %s", proto.MarshalTextString(response))
			// result := response.GetResult()
			// printOutputFile(ctx, bsClient, result.GetStdoutDigest(), "stdout")
			// printOutputFile(ctx, bsClient, result.GetStderrDigest(), "stderr")
			// }
			// logActionResult(d, response.GetResult().GetExecutionMetadata())
			break
		}
	}
}
