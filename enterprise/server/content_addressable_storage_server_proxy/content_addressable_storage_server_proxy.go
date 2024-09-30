package content_addressable_storage_server_proxy

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strconv"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/rpcutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var enableGetTreeCaching = flag.Bool("cache_proxy.enable_get_tree_caching", false, "If true, the Cache Proxy attempts to serve GetTree requests out of the local cache. If false, GetTree requests are always proxied to the remote, authoritative cache.")

type CASServerProxy struct {
	atimeUpdater interfaces.AtimeUpdater
	local        repb.ContentAddressableStorageClient
	remote       repb.ContentAddressableStorageClient
}

func Register(env *real_environment.RealEnv) error {
	casServer, err := New(env)
	if err != nil {
		return status.InternalErrorf("Error initializing ContentAddressableStorageServerProxy: %s", err)
	}
	env.SetCASServer(casServer)
	return nil
}

func New(env environment.Env) (*CASServerProxy, error) {
	atimeUpdater := env.GetAtimeUpdater()
	if atimeUpdater == nil {
		return nil, fmt.Errorf("An AtimeUpdater is required to enable the ContentAddressableStorageServerProxy")
	}
	local := env.GetLocalCASClient()
	if local == nil {
		return nil, fmt.Errorf("A local ContentAddressableStorageClient is required to enable the ContentAddressableStorageServerProxy")
	}
	remote := env.GetContentAddressableStorageClient()
	if remote == nil {
		return nil, fmt.Errorf("A remote ContentAddressableStorageClient is required to enable the ContentAddressableStorageServerProxy")
	}
	proxy := CASServerProxy{
		atimeUpdater: atimeUpdater,
		local:        local,
		remote:       remote,
	}
	return &proxy, nil
}

func recordMetrics(op, status string, perDigestStatus map[string]int) {
	metrics.ContentAddressableStorageProxyReads.With(
		prometheus.Labels{
			metrics.CASOperation:       op,
			metrics.CacheHitMissStatus: status,
		}).Inc()
	for status, count := range perDigestStatus {
		metrics.ContentAddressableStorageProxyDigestReads.With(
			prometheus.Labels{
				metrics.CASOperation:       op,
				metrics.CacheHitMissStatus: status,
			}).Add(float64(count))
	}
}

func (s *CASServerProxy) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	tracing.AddStringAttributeToCurrentSpan(ctx, "requested-blobs", strconv.Itoa(len(req.BlobDigests)))

	// TODO(iain): This will over-aggressively update remote atimes. If it's a
	// problem, we can change the logic around to only update atimes for blobs
	// that were found locally.
	s.atimeUpdater.EnqueueByFindMissingRequest(ctx, req)

	resp, err := s.local.FindMissingBlobs(ctx, req)
	if err != nil {
		return nil, err
	}
	tracing.AddStringAttributeToCurrentSpan(ctx, "locally-missing-blobs", strconv.Itoa(len(resp.MissingBlobDigests)))
	if len(resp.MissingBlobDigests) == 0 {
		recordMetrics("FindMissingBlobs", "hit", map[string]int{"hit": len(req.BlobDigests)})
		return resp, nil
	}

	if len(resp.MissingBlobDigests) == len(req.BlobDigests) {
		recordMetrics("FindMissingBlobs", "miss", map[string]int{"miss": len(req.BlobDigests)})
	} else {
		recordMetrics("FindMissingBlobs", "partial", map[string]int{
			"hit":  len(req.BlobDigests) - len(resp.MissingBlobDigests),
			"miss": len(resp.MissingBlobDigests),
		})
	}

	remoteReq := repb.FindMissingBlobsRequest{
		InstanceName:   req.InstanceName,
		BlobDigests:    resp.MissingBlobDigests,
		DigestFunction: req.DigestFunction,
	}
	return s.remote.FindMissingBlobs(ctx, &remoteReq)
}

func (s *CASServerProxy) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	_, err := s.local.BatchUpdateBlobs(ctx, req)
	if err != nil {
		log.Warningf("Local BatchUpdateBlobs error: %s", err)
	}
	return s.remote.BatchUpdateBlobs(ctx, req)
}

func (s *CASServerProxy) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	tracing.AddStringAttributeToCurrentSpan(ctx, "requested-blobs", strconv.Itoa(len(req.Digests)))

	mergedResp := repb.BatchReadBlobsResponse{}
	mergedDigests := []*repb.Digest{}
	localResp, err := s.local.BatchReadBlobs(ctx, req)
	if err != nil {
		recordMetrics("BatchReadBlobs", "miss", map[string]int{"miss": len(req.Digests)})
		return s.batchReadBlobsRemote(ctx, req)
	}
	for _, resp := range localResp.Responses {
		if resp.Status.Code == int32(codes.OK) {
			mergedResp.Responses = append(mergedResp.Responses, resp)
			mergedDigests = append(mergedDigests, resp.Digest)
		}
	}
	s.atimeUpdater.Enqueue(ctx, req.InstanceName, mergedDigests, req.DigestFunction)
	if len(mergedResp.Responses) == len(req.Digests) {
		recordMetrics("BatchReadBlobs", "hit", map[string]int{"hit": len(req.Digests)})
		return &mergedResp, nil
	}

	recordMetrics("BatchReadBlobs", "partial", map[string]int{
		"hit":  len(mergedResp.Responses),
		"miss": len(req.Digests) - len(mergedResp.Responses),
	})

	// digest.Diff returns a set of differences between two sets of digests,
	// but the protocol requires the server return multiple responses if the
	// same digest is requested multiple times. Count the number of client
	// requests per blob so we can duplicate responses that many times before
	// returning to the client.
	_, missing := digest.Diff(req.Digests, mergedDigests)
	cardinality := make(map[digest.Key]int)
	for _, d := range req.Digests {
		k := digest.NewKey(d)
		if _, ok := cardinality[k]; ok {
			cardinality[k] = cardinality[k] + 1
		} else {
			cardinality[k] = 1
		}
	}
	remoteReq := repb.BatchReadBlobsRequest{
		InstanceName:          req.InstanceName,
		Digests:               missing,
		AcceptableCompressors: req.AcceptableCompressors,
		DigestFunction:        req.DigestFunction,
	}
	remoteResp, err := s.batchReadBlobsRemote(ctx, &remoteReq)
	if err != nil {
		return nil, err
	}

	// Now go through and duplicate each response as many times as the client
	// requested it.
	for _, response := range remoteResp.Responses {
		c, ok := cardinality[digest.NewKey(response.Digest)]
		if !ok {
			log.Warningf("Received unexpected digest from remote CAS.BatchReadBlobs: %s/%d", response.Digest.Hash, response.Digest.SizeBytes)
		}
		for i := 0; i < c; i++ {
			mergedResp.Responses = append(mergedResp.Responses, response)
		}
	}

	return &mergedResp, nil
}

func (s *CASServerProxy) batchReadBlobsRemote(ctx context.Context, readReq *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	tracing.AddStringAttributeToCurrentSpan(ctx, "requested-blobs", strconv.Itoa(len(readReq.Digests)))
	readResp, err := s.remote.BatchReadBlobs(ctx, readReq)
	if err != nil {
		return nil, err
	}
	updateReq := repb.BatchUpdateBlobsRequest{
		InstanceName:   readReq.InstanceName,
		DigestFunction: readReq.DigestFunction,
	}
	for _, response := range readResp.Responses {
		if response.Status.Code != int32(codes.OK) {
			continue
		}
		updateReq.Requests = append(updateReq.Requests, &repb.BatchUpdateBlobsRequest_Request{
			Digest:     response.Digest,
			Data:       response.Data,
			Compressor: response.Compressor,
		})
	}
	if _, err := s.local.BatchUpdateBlobs(ctx, &updateReq); err != nil {
		log.Warningf("Error locally updating blobs: %s", err)
	}
	return readResp, nil
}

func (s *CASServerProxy) GetTree(req *repb.GetTreeRequest, stream repb.ContentAddressableStorage_GetTreeServer) error {
	if *enableGetTreeCaching {
		return s.getTree(req, stream)
	}
	return s.getTreeWithoutCaching(req, stream)
}

func (s *CASServerProxy) getTreeWithoutCaching(req *repb.GetTreeRequest, stream repb.ContentAddressableStorage_GetTreeServer) error {
	remoteStream, err := s.remote.GetTree(stream.Context(), req)
	if err != nil {
		return err
	}
	for {
		rsp, err := remoteStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err = stream.Send(rsp); err != nil {
			return err
		}
	}
	return nil
}

func (s *CASServerProxy) getTree(req *repb.GetTreeRequest, stream repb.ContentAddressableStorage_GetTreeServer) error {
	ctx, spn := tracing.StartSpan(stream.Context())
	defer spn.End()

	resp := repb.GetTreeResponse{}
	respSizeBytes := 0
	for dirsToGet := []*repb.Digest{req.RootDigest}; len(dirsToGet) > 0; {
		brbreq := repb.BatchReadBlobsRequest{
			InstanceName:   req.InstanceName,
			Digests:        dirsToGet,
			DigestFunction: req.DigestFunction,
		}
		brbresps, err := s.BatchReadBlobs(ctx, &brbreq)
		if err != nil {
			return err
		}

		dirsToGet = []*repb.Digest{}
		for _, brbresp := range brbresps.Responses {
			dir := &repb.Directory{}
			if err := proto.Unmarshal(brbresp.Data, dir); err != nil {
				return err
			}

			// Flush to the stream if adding the dir will make resp bigger than
			// the maximum gRPC frame size.
			dirSizeBytes := proto.Size(dir)
			if int64(respSizeBytes+dirSizeBytes) > rpcutil.GRPCMaxSizeBytes {
				if err := stream.Send(&resp); err != nil {
					return err
				}
				resp = repb.GetTreeResponse{}
				respSizeBytes = 0
			}

			resp.Directories = append(resp.Directories, dir)
			respSizeBytes += dirSizeBytes
			for _, subDir := range dir.Directories {
				dirsToGet = append(dirsToGet, subDir.Digest)
			}
		}
	}
	return stream.Send(&resp)
}
