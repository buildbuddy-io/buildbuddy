package cache_proxy

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	smpb "github.com/buildbuddy-io/buildbuddy/proto/semver"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	readThrough        = flag.Bool("local_cache_proxy.read_through", true, "If true, cache remote reads locally")
	writeThrough       = flag.Bool("local_cache_proxy.write_through", true, "If true, upload writes to remote cache too")
	synchronousWrite   = flag.Bool("local_cache_proxy.synchronous_write", false, "If true, wait until writes to remote cache are finished")
	slowWriteThreshold = flag.Duration("local_cache_proxy.slow_write_threshold", 30*time.Second, "If greater than 0, log warnings for cache writes that take longer than this duration")
)

const (
	queueBufferSize = 10_000
)

// CacheProxy implements a local GRPC cache that proxies a remote GRPC cache.
// It implements both read-through and write-through functionality by:
//   - Checking existence first against the local cache, then, if any keys are
//     still not found, consulting the remote cache.
//   - Reading first from the local cache, then, if a key is not found, reading
//     from the remote cache and writing the fetched object to the local cache.
//   - Writing to the local cache and returning success immediately to the
//     client, then enqueueing a job to upload this key to the remote cache.
type CacheProxy struct {
	acClient  repb.ActionCacheClient
	bsClient  bspb.ByteStreamClient
	casClient repb.ContentAddressableStorageClient
	cpbClient repb.CapabilitiesClient

	env            environment.Env
	cache          interfaces.Cache
	localBSS       *byte_stream_server.ByteStreamServer
	localCAS       *content_addressable_storage_server.ContentAddressableStorageServer
	localBSSClient bspb.ByteStreamClient

	qWorker *queueWorker
}

// startServerLocally registers the localBSS and serves it over a bufconn
// (in-memory connection) that is returned to the caller. The server is shutdown
// when the provided context is cancelled.
func startServerLocally(ctx context.Context, localBSS bspb.ByteStreamServer) (*grpc.ClientConn, error) {
	buffer := 1024 * 1024 * 10
	listener := bufconn.Listen(buffer)

	localGRPCServer := grpc.NewServer()
	bspb.RegisterByteStreamServer(localGRPCServer, localBSS)
	go func() {
		if err := localGRPCServer.Serve(listener); err != nil {
			log.Errorf("error serving locally: %s", err.Error())
		}
		listener.Close()
		localGRPCServer.Stop()
	}()

	conn, err := grpc.DialContext(ctx, "", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func NewCacheProxy(ctx context.Context, env environment.Env, conn grpc.ClientConnInterface) (*CacheProxy, error) {
	if env.GetCache() == nil {
		return nil, status.FailedPreconditionError("CacheProxy requires a local cache to run.")
	}
	cache := env.GetCache()
	localBSS, err := byte_stream_server.NewByteStreamServer(env)
	if err != nil {
		return nil, status.InternalErrorf("CacheProxy: error starting local bytestream server: %s", err.Error())
	}
	localCAS, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	if err != nil {
		return nil, status.InternalErrorf("CacheProxy: error starting local CAS server: %s", err.Error())
	}
	localConn, err := startServerLocally(ctx, localBSS)
	if err != nil {
		return nil, status.InternalErrorf("CacheProxy: error starting local bytestream gRPC server: %s", err.Error())
	}
	localBSSClient := bspb.NewByteStreamClient(localConn)
	remoteBSSClient := bspb.NewByteStreamClient(conn)
	return &CacheProxy{
		acClient:       repb.NewActionCacheClient(conn),
		bsClient:       remoteBSSClient,
		casClient:      repb.NewContentAddressableStorageClient(conn),
		cpbClient:      repb.NewCapabilitiesClient(conn),
		env:            env,
		cache:          cache,
		localBSS:       localBSS,
		localCAS:       localCAS,
		localBSSClient: localBSSClient,
		qWorker:        NewQueueWorker(ctx, localBSSClient, remoteBSSClient),
	}, nil
}

func (p *CacheProxy) GetCapabilities(ctx context.Context, req *repb.GetCapabilitiesRequest) (*repb.ServerCapabilities, error) {
	res, err := p.cpbClient.GetCapabilities(ctx, req)
	if err != nil {
		d := defaultCapabilities()
		log.Warningf("Failed to fetch capabilities from remote server due to %s, returning default capabilities: %+v", err, d)
		return d, nil
	}
	if b, err := protojson.Marshal(res); err == nil {
		log.Infof("Remote capabilities: %s", string(b))
	}
	return res, err
}

func (p *CacheProxy) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest) (*repb.ActionResult, error) {
	return p.acClient.GetActionResult(ctx, req)
}

func (p *CacheProxy) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest) (*repb.ActionResult, error) {
	return p.acClient.UpdateActionResult(ctx, req)
}

func (p *CacheProxy) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	return p.casClient.BatchUpdateBlobs(ctx, req)
}

func (p *CacheProxy) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	return p.casClient.BatchReadBlobs(ctx, req)
}

func (p *CacheProxy) GetTree(req *repb.GetTreeRequest, stream repb.ContentAddressableStorage_GetTreeServer) error {
	clientStream, err := p.casClient.GetTree(stream.Context(), req)
	if err != nil {
		return err
	}
	for {
		msg, err := clientStream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if err := stream.Send(msg); err != nil {
			return err
		}
	}
	return nil
}

func (p *CacheProxy) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	return p.casClient.FindMissingBlobs(ctx, req)
}

func (p *CacheProxy) hasBlobLocally(ctx context.Context, instanceName string, d *repb.Digest) bool {
	rsp, err := p.localCAS.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
		InstanceName: instanceName,
		BlobDigests:  []*repb.Digest{d},
	})

	if err == nil && len(rsp.GetMissingBlobDigests()) == 0 {
		return true
	}
	return false
}

func (p *CacheProxy) Read(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	ctx := stream.Context()
	resourceName, err := digest.ParseDownloadResourceName(req.GetResourceName())
	d := resourceName.GetDigest()
	instanceName := resourceName.GetInstanceName()
	if err == nil && p.hasBlobLocally(ctx, instanceName, d) {
		return p.localBSS.Read(req, stream)
	}
	clientStream, err := p.bsClient.Read(ctx, req)
	if err != nil {
		return err
	}

	var localFile *os.File
	if *readThrough {
		tmpFile, err := os.CreateTemp("", fmt.Sprintf("%s%s-", instanceName, d.GetHash()))
		if err == nil {
			localFile = tmpFile
			defer os.Remove(tmpFile.Name())
		}
	}
	for {
		msg, err := clientStream.Recv()
		if err != nil {
			if err == io.EOF {
				if localFile != nil {
					resourceName := digest.NewResourceName(d, instanceName, rspb.CacheType_CAS, resourceName.GetDigestFunction())
					if _, _, err := cachetools.UploadFromReader(ctx, p.localBSSClient, resourceName, localFile); err != nil {
						log.Errorf("error uploading from reader: %s", err.Error())
					}
				}
				break
			}
			return err
		}
		if sendErr := stream.Send(msg); sendErr != nil {
			return sendErr
		}
		if localFile != nil {
			if _, err := localFile.Write(msg.GetData()); err != nil {
				log.Errorf("error writing data to local file: %s", err.Error())
			}
		}
	}
	return nil
}

func (p *CacheProxy) Write(stream bspb.ByteStream_WriteServer) error {
	ctx := stream.Context()
	var wreq *bspb.WriteRequest
	clientStream, err := p.localBSSClient.Write(ctx)
	if err != nil {
		return err
	}
	for {
		rsp, err := stream.Recv()
		if err != nil {
			return err
		}
		if wreq == nil {
			wreq = rsp
		}
		writeDone := rsp.GetFinishWrite()
		if err := clientStream.Send(rsp); err != nil {
			if err == io.EOF {
				writeDone = true
			} else {
				return err
			}
		}
		if writeDone {
			lastRsp, err := clientStream.CloseAndRecv()
			if err != nil {
				return err
			}
			if *writeThrough {
				if *synchronousWrite {
					if err := p.qWorker.RemoteWriteBlocked(ctx, wreq); err != nil {
						log.CtxErrorf(ctx, "Error write to remote cache: %s", err)
					}
				} else {
					if err := p.qWorker.EnqueueRemoteWrite(ctx, wreq); err != nil {
						log.CtxErrorf(ctx, "Error enqueueing write request to remote: %s", err.Error())
					}
				}
			}
			return stream.SendAndClose(lastRsp)
		}
	}
}

func (p *CacheProxy) QueryWriteStatus(ctx context.Context, req *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	return p.localBSS.QueryWriteStatus(ctx, req)
}

func logContextFromMetadata(ctx context.Context, md metadata.MD) context.Context {
	if md == nil {
		return ctx
	}
	if iid := getInvocationID(md); iid != "" {
		ctx = log.EnrichContext(ctx, log.InvocationIDKey, iid)
	}
	return ctx
}

func getInvocationID(md metadata.MD) string {
	vals := md[bazel_request.RequestMetadataKey]
	if len(vals) == 0 {
		return ""
	}
	rmd := &repb.RequestMetadata{}
	b := []byte(vals[0])
	if err := proto.Unmarshal(b, rmd); err != nil {
		return ""
	}
	return rmd.GetToolInvocationId()
}

type queueReq struct {
	metadata          metadata.MD
	writeResourceName string
}
type queueWorker struct {
	ctx          context.Context
	workQ        chan queueReq
	localClient  bspb.ByteStreamClient
	remoteClient bspb.ByteStreamClient
}

func NewQueueWorker(ctx context.Context, localClient, remoteClient bspb.ByteStreamClient) *queueWorker {
	qw := &queueWorker{
		ctx:          ctx,
		workQ:        make(chan queueReq, queueBufferSize),
		localClient:  localClient,
		remoteClient: remoteClient,
	}
	qw.Start()
	return qw
}

func (qw *queueWorker) Start() {
	go func() {
		for {
			select {
			case <-qw.ctx.Done():
				return
			case req := <-qw.workQ:
				ctx := qw.ctx
				// Reconstruct the original context, including gRPC metadata
				// and logging context that would normally be populated via
				// the client interceptors.
				ctx = metadata.NewOutgoingContext(ctx, req.metadata)
				ctx = logContextFromMetadata(ctx, req.metadata)

				if err := qw.handleWriteRequest(ctx, req.writeResourceName); err != nil {
					log.CtxErrorf(ctx, "Error handling write request: %s", err)
				}
			}
		}
	}()
}

func (qw *queueWorker) handleWriteRequest(ctx context.Context, writeResourceName string) error {
	if *slowWriteThreshold > 0 {
		ticker := time.NewTicker(*slowWriteThreshold)
		defer ticker.Stop()
		done := make(chan struct{})
		defer close(done)
		go func() {
			start := time.Now()
			for {
				select {
				case <-ticker.C:
					log.CtxWarningf(ctx, "Slow cache upload: %q still in progress after %.1fs", writeResourceName, time.Since(start).Seconds())
				case <-done:
					return
				}
			}
		}()
	}

	start := time.Now()
	resourceName, err := digest.ParseUploadResourceName(writeResourceName)
	if err != nil {
		return err
	}
	d := resourceName.GetDigest()
	instanceName := resourceName.GetInstanceName()
	tmpFile, err := os.CreateTemp("", fmt.Sprintf("%s%s-", instanceName, d.GetHash()))
	if err != nil {
		return err
	}
	defer os.Remove(tmpFile.Name())
	if err := cachetools.GetBlob(ctx, qw.localClient, resourceName, tmpFile); err != nil {
		return err
	}
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, _, err := cachetools.UploadFromReader(ctx, qw.remoteClient, resourceName, tmpFile); err != nil {
		return err
	}
	log.CtxDebugf(ctx, "Wrote %q in %s", writeResourceName, time.Since(start))
	return nil
}

func (qw *queueWorker) EnqueueRemoteWrite(ctx context.Context, wreq *bspb.WriteRequest) error {
	md, _ := metadata.FromIncomingContext(ctx)
	req := queueReq{
		metadata:          md,
		writeResourceName: wreq.GetResourceName(),
	}
	select {
	case qw.workQ <- req:
		return nil
	default:
		return status.ResourceExhaustedError("Queue was at capacity.")
	}
}

func (qw *queueWorker) RemoteWriteBlocked(ctx context.Context, wreq *bspb.WriteRequest) error {
	// Copy all incoming metadata to outgoing RPCs.
	md, _ := metadata.FromIncomingContext(ctx)
	ctx = metadata.NewOutgoingContext(ctx, md)

	return qw.handleWriteRequest(ctx, wreq.GetResourceName())
}

func defaultCapabilities() *repb.ServerCapabilities {
	var compressors = []repb.Compressor_Value{repb.Compressor_IDENTITY, repb.Compressor_ZSTD}
	return &repb.ServerCapabilities{
		// Support REAPI 2.0 -> 2.3
		LowApiVersion:  &smpb.SemVer{Major: int32(2)},
		HighApiVersion: &smpb.SemVer{Major: int32(2), Minor: int32(3)},
		CacheCapabilities: &repb.CacheCapabilities{
			DigestFunctions: digest.SupportedDigestFunctions(),
			ActionCacheUpdateCapabilities: &repb.ActionCacheUpdateCapabilities{
				UpdateEnabled: true,
			},
			CachePriorityCapabilities: &repb.PriorityCapabilities{
				Priorities: []*repb.PriorityCapabilities_PriorityRange{
					{
						MinPriority: 0,
						MaxPriority: 0,
					},
				},
			},
			MaxBatchTotalSizeBytes:          0, // Default to protocol limit.
			SymlinkAbsolutePathStrategy:     repb.SymlinkAbsolutePathStrategy_ALLOWED,
			SupportedCompressors:            compressors,
			SupportedBatchUpdateCompressors: compressors,
		},
		ExecutionCapabilities: &repb.ExecutionCapabilities{
			DigestFunction: repb.DigestFunction_SHA256,
			ExecEnabled:    true,
			ExecutionPriorityCapabilities: &repb.PriorityCapabilities{
				Priorities: []*repb.PriorityCapabilities_PriorityRange{
					{MinPriority: math.MinInt32, MaxPriority: math.MaxInt32},
				},
			},
			DigestFunctions: digest.SupportedDigestFunctions(),
		},
	}
}
