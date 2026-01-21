package content_addressable_storage_server_proxy

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strconv"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_crypter"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/proxy_util"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/chunked_manifest"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
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
	supportsEncryption func(context.Context) bool
	authenticator      interfaces.Authenticator
	local              repb.ContentAddressableStorageServer
	remote             repb.ContentAddressableStorageClient
	localCache         interfaces.Cache
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
	authenticator := env.GetAuthenticator()
	if authenticator == nil {
		return nil, fmt.Errorf("An Authenticator is required to enable the ContentAddressableStorageServerProxy")
	}
	local := env.GetLocalCASServer()
	if local == nil {
		return nil, fmt.Errorf("A local ContentAddressableStorageClient is required to enable the ContentAddressableStorageServerProxy")
	}
	remote := env.GetContentAddressableStorageClient()
	if remote == nil {
		return nil, fmt.Errorf("A remote ContentAddressableStorageClient is required to enable the ContentAddressableStorageServerProxy")
	}
	proxy := CASServerProxy{
		supportsEncryption: remote_crypter.SupportsEncryption(env),
		authenticator:      authenticator,
		local:              local,
		remote:             remote,
		localCache:         env.GetCache(),
	}
	return &proxy, nil
}

type cacheMetrics struct {
	digestsPerStatusAndCompressor map[string]map[string]int
	bytesPerStatusAndCompressor   map[string]map[string]int
}

func newCacheMetrics() *cacheMetrics {
	return &cacheMetrics{
		digestsPerStatusAndCompressor: map[string]map[string]int{
			metrics.HitStatusLabel:         map[string]int{},
			metrics.MissStatusLabel:        map[string]int{},
			metrics.UncacheableStatusLabel: map[string]int{},
		},
		bytesPerStatusAndCompressor: map[string]map[string]int{
			metrics.HitStatusLabel:         map[string]int{},
			metrics.MissStatusLabel:        map[string]int{},
			metrics.UncacheableStatusLabel: map[string]int{},
		},
	}
}

func (m *cacheMetrics) addUpdateMetrics(requests []*repb.BatchUpdateBlobsRequest_Request) *cacheMetrics {
	status := metrics.MissStatusLabel
	for _, request := range requests {
		compressor := request.GetCompressor().String()
		m.digestsPerStatusAndCompressor[status][compressor]++
		m.bytesPerStatusAndCompressor[status][compressor] += len(request.Data)
	}
	return m
}

func (m *cacheMetrics) addReadMetrics(status string, responses []*repb.BatchReadBlobsResponse_Response) *cacheMetrics {
	for _, response := range responses {
		compressor := response.GetCompressor().String()
		m.digestsPerStatusAndCompressor[status][compressor]++
		m.bytesPerStatusAndCompressor[status][compressor] += len(response.Data)
	}
	return m
}

func (m *cacheMetrics) addGetTreeMetrics(digests, bytes int) *cacheMetrics {
	status := metrics.MissStatusLabel
	compressor := repb.Compressor_IDENTITY.String()
	m.digestsPerStatusAndCompressor[status][compressor] += digests
	m.bytesPerStatusAndCompressor[status][compressor] += bytes
	return m
}

func recordMetrics(op, status string, cm *cacheMetrics) {
	metrics.ContentAddressableStorageProxiedRequests.With(
		prometheus.Labels{
			metrics.CASOperation:       op,
			metrics.CacheHitMissStatus: status,
		}).Inc()
	for status, digestsPerCompressor := range cm.digestsPerStatusAndCompressor {
		for compressor, count := range digestsPerCompressor {
			metrics.ContentAddressableStorageProxiedDigests.With(
				prometheus.Labels{
					metrics.CASOperation:       op,
					metrics.CacheHitMissStatus: status,
					metrics.CompressionType:    compressor,
				}).Add(float64(count))
		}
	}
	for status, bytesPerCompressor := range cm.bytesPerStatusAndCompressor {
		for compressor, bytes := range bytesPerCompressor {
			metrics.ContentAddressableStorageProxiedBytes.With(
				prometheus.Labels{
					metrics.CASOperation:       op,
					metrics.CacheHitMissStatus: status,
					metrics.CompressionType:    compressor,
				}).Add(float64(bytes))
		}
	}
}

func (s *CASServerProxy) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	if proxy_util.SkipRemote(ctx) {
		return s.local.FindMissingBlobs(ctx, req)
	}

	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	tracing.AddStringAttributeToCurrentSpan(ctx, "requested-blobs", strconv.Itoa(len(req.BlobDigests)))

	// Always serve FindMissingBlobs requests out of the backing cache to
	// avoid possible cache-inconsistency bugs.
	return s.remote.FindMissingBlobs(ctx, req)
}

func (s *CASServerProxy) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	if proxy_util.SkipRemote(ctx) {
		return nil, status.UnimplementedError("Skip remote not implemented")
	}

	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()

	recordMetrics("BatchUpdateBlobs", metrics.MissStatusLabel, newCacheMetrics().addUpdateMetrics(req.Requests))

	if authutil.EncryptionEnabled(ctx, s.authenticator) && !s.supportsEncryption(ctx) {
		return s.remote.BatchUpdateBlobs(ctx, req)
	}

	_, err := s.local.BatchUpdateBlobs(ctx, req)
	if err != nil {
		log.CtxWarningf(ctx, "Local BatchUpdateBlobs error: %s", err)
	}
	return s.remote.BatchUpdateBlobs(ctx, req)
}

func bytesInRequest(req *repb.BatchUpdateBlobsRequest) int {
	if req == nil {
		return 0
	}
	bytes := 0
	for _, req := range req.Requests {
		bytes += len(req.GetData())
	}
	return bytes
}

func bytesInResponse(resp *repb.BatchReadBlobsResponse) int {
	if resp == nil {
		return 0
	}
	bytes := 0
	for _, response := range resp.Responses {
		bytes += len(response.GetData())
	}
	return bytes
}

func (s *CASServerProxy) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	if proxy_util.SkipRemote(ctx) {
		return nil, status.UnimplementedError("Skip remote not implemented")
	}

	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()
	tracing.AddStringAttributeToCurrentSpan(ctx, "requested-blobs", strconv.Itoa(len(req.Digests)))

	// Store auth headers in context so they can be reused between the
	// atime_updater and the hit_tracker_client.
	ctx = authutil.ContextWithCachedAuthHeaders(ctx, s.authenticator)

	mergedResp := repb.BatchReadBlobsResponse{}
	mergedDigests := []*repb.Digest{}
	localResp := &repb.BatchReadBlobsResponse{}
	remoteOnly := authutil.EncryptionEnabled(ctx, s.authenticator) && !s.supportsEncryption(ctx)
	if !remoteOnly {
		resp, err := s.local.BatchReadBlobs(ctx, req)
		if err != nil {
			recordMetrics(
				"BatchReadBlobs",
				metrics.MissStatusLabel,
				newCacheMetrics().addReadMetrics(metrics.MissStatusLabel, resp.GetResponses()))
			return s.batchReadBlobsRemote(ctx, req)
		}
		localResp = resp
	}
	for _, resp := range localResp.Responses {
		if resp.Status.Code == int32(codes.OK) {
			mergedResp.Responses = append(mergedResp.Responses, resp)
			mergedDigests = append(mergedDigests, resp.Digest)
		}
	}

	cacheMetrics := newCacheMetrics().addReadMetrics(metrics.HitStatusLabel, mergedResp.GetResponses())
	if len(mergedResp.Responses) == len(req.Digests) {
		recordMetrics("BatchReadBlobs", metrics.HitStatusLabel, cacheMetrics)
		return &mergedResp, nil
	}

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
		// Don't record metrics here (for now at least)
		return nil, err
	}

	// Now go through and duplicate each response as many times as the client
	// requested it.
	for _, response := range remoteResp.Responses {
		c, ok := cardinality[digest.NewKey(response.Digest)]
		if !ok {
			log.CtxWarningf(ctx, "Received unexpected digest from remote CAS.BatchReadBlobs: %s/%d", response.Digest.Hash, response.Digest.SizeBytes)
		}
		for i := 0; i < c; i++ {
			mergedResp.Responses = append(mergedResp.Responses, response)
		}
	}

	if remoteOnly {
		cacheMetrics.addReadMetrics(metrics.UncacheableStatusLabel, mergedResp.Responses)
		recordMetrics("BatchReadBlobs", metrics.UncacheableStatusLabel, cacheMetrics)
	} else {
		cacheMetrics.addReadMetrics(metrics.MissStatusLabel, remoteResp.Responses)
		recordMetrics("BatchReadBlobs", metrics.PartialStatusLabel, cacheMetrics)
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
	if !authutil.EncryptionEnabled(ctx, s.authenticator) || s.supportsEncryption(ctx) {
		if _, err := s.local.BatchUpdateBlobs(ctx, &updateReq); err != nil {
			log.CtxWarningf(ctx, "Error locally updating blobs: %s", err)
		}
	}
	return readResp, nil
}

func (s *CASServerProxy) GetTree(req *repb.GetTreeRequest, stream repb.ContentAddressableStorage_GetTreeServer) error {
	if proxy_util.SkipRemote(stream.Context()) {
		return status.UnimplementedError("Skip remote not implemented")
	}

	if *enableGetTreeCaching {
		return s.getTree(req, stream)
	}
	return s.getTreeWithoutCaching(req, stream)
}

func (s *CASServerProxy) getTreeWithoutCaching(req *repb.GetTreeRequest, stream repb.ContentAddressableStorage_GetTreeServer) error {
	digests := 0
	bytes := 0
	defer func() {
		recordMetrics("GetTree", metrics.MissStatusLabel,
			newCacheMetrics().addGetTreeMetrics(digests, bytes))
	}()
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
		for _, dir := range rsp.GetDirectories() {
			digests += len(dir.GetFiles())
			digests += len(dir.GetDirectories())
		}
		bytes += proto.Size(rsp)
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

func (s *CASServerProxy) SpliceBlob(ctx context.Context, req *repb.SpliceBlobRequest) (*repb.SpliceBlobResponse, error) {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()

	if proxy_util.SkipRemote(ctx) {
		return nil, status.UnimplementedErrorf("SpliceBlob RPC is not supported for skipping remote")
	}

	// The local proxy might not have all chunks available,
	// so call SpliceBlob on remote only.
	return s.remote.SpliceBlob(ctx, req)
}

func (s *CASServerProxy) SplitBlob(ctx context.Context, req *repb.SplitBlobRequest) (*repb.SplitBlobResponse, error) {
	ctx, spn := tracing.StartSpan(ctx)
	defer spn.End()

	if proxy_util.SkipRemote(ctx) {
		return nil, status.UnimplementedErrorf("SplitBlob RPC is not supported for skipping remote")
	}

	localResp, localErr := s.local.SplitBlob(ctx, req)
	if localErr == nil {
		return localResp, nil
	}
	if !status.IsNotFoundError(localErr) {
		return nil, status.WrapError(localErr, "local SplitBlob")
	}

	remoteResp, remoteErr := s.remote.SplitBlob(ctx, req)
	if remoteErr != nil {
		return nil, status.WrapError(remoteErr, "remote SplitBlob")
	}

	// Store the manifest locally, skipping validation since it was already
	// validated by the remote.
	if s.localCache != nil {
		ctx, err := prefix.AttachUserPrefixToContext(ctx, s.authenticator)
		if err != nil {
			return nil, err
		}
		manifest := chunked_manifest.FromSplitResponse(req, remoteResp)
		if err := manifest.StoreWithoutValidation(ctx, s.localCache); err != nil {
			return nil, status.WrapError(err, "SplitBlob store remote manifest to local")
		}
	}
	return remoteResp, nil
}
