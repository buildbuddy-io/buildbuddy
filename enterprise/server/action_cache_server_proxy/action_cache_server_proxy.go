package action_cache_server_proxy

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gstatus "google.golang.org/grpc/status"
)

var inlineActionOutputsInProxy = flag.Bool("cache_proxy.inline_action_outputs_in_proxy", false, "If true, the Cache Proxy will fill in inline_output_files instead of forwarding the list to the remote cache.")

type ActionCacheServerProxy struct {
	env         environment.Env
	remoteCache repb.ActionCacheClient
}

func Register(env *real_environment.RealEnv) error {
	actionCacheServer, err := NewActionCacheServerProxy(env)
	if err != nil {
		return status.InternalErrorf("Error initializing ActionCacheServerProxy: %s", err)
	}
	env.SetActionCacheServer(actionCacheServer)
	return nil
}

func NewActionCacheServerProxy(env environment.Env) (*ActionCacheServerProxy, error) {
	remoteCache := env.GetActionCacheClient()
	if remoteCache == nil {
		return nil, fmt.Errorf("An ActionCacheClient is required to enable the ActionCacheServerProxy")
	}
	return &ActionCacheServerProxy{
		env:         env,
		remoteCache: remoteCache,
	}, nil
}

// Action Cache entries are not content-addressable, so the value pointed to
// by a given key may change in the backing cache. Thus, always fetch them from
// the authoritative cache.
func (s *ActionCacheServerProxy) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest) (*repb.ActionResult, error) {
	filesToInline := req.GetInlineOutputFiles()
	if *inlineActionOutputsInProxy {
		req.InlineOutputFiles = []string{}
	}
	resp, err := s.remoteCache.GetActionResult(ctx, req)
	if *inlineActionOutputsInProxy && err == nil && len(filesToInline) > 0 {
		// The default limit on incoming gRPC messages is 4MB and Bazel doesn't
		// change it.
		req.InlineOutputFiles = filesToInline
		s.maybeInlineOutputFiles(ctx, req, resp, 4*1024*1024)
	}
	labels := prometheus.Labels{
		metrics.StatusLabel:        fmt.Sprintf("%d", gstatus.Code(err)),
		metrics.CacheHitMissStatus: "miss",
	}
	metrics.ActionCacheProxiedReadRequests.With(labels).Inc()
	metrics.ActionCacheProxiedReadByes.With(labels).Add(float64(proto.Size(resp)))
	return resp, err
}

// TODO(jdhollen): This could probably be shared with the matching logic in
// action_cache_server, but this code calls via BatchReadBlobs so that it can
// hit proxy methods, so it's not _exactly_ the same.
func (s *ActionCacheServerProxy) maybeInlineOutputFiles(ctx context.Context, req *repb.GetActionResultRequest, ar *repb.ActionResult, maxResultSize int) error {
	if ar == nil || len(req.InlineOutputFiles) == 0 {
		return nil
	}
	requestedFiles := make(map[string]struct{}, len(req.InlineOutputFiles))
	for _, f := range req.InlineOutputFiles {
		requestedFiles[f] = struct{}{}
	}

	budget := int64(max(0, maxResultSize-proto.Size(ar)))
	var filesToInline []*repb.OutputFile
	for i, f := range ar.OutputFiles {
		if _, ok := requestedFiles[f.Path]; !ok {
			continue
		}
		contentsSize := f.GetDigest().GetSizeBytes()
		if contentsSize == 0 {
			// Empty files don't need to be inlined as they can be recognized
			// by their size.
			continue
		}

		if i == 0 {
			// Bazel only requests inlining for a single file and we may exit
			// this loop early, so don't track sizes for the other files.
			metrics.CacheRequestedInlineSizeBytes.With(prometheus.Labels{}).Observe(float64(contentsSize))
		}

		// An additional "contents" field requires 1 byte for the tag field
		// (5:LEN), the bytes for the varint encoding of the length of the
		// contents and the contents themselves.
		totalSize := 1 + int64(len(binary.AppendUvarint(nil, uint64(contentsSize)))) + contentsSize
		if budget < totalSize {
			continue
		}
		budget -= totalSize
		filesToInline = append(filesToInline, f)
	}

	if len(filesToInline) == 0 {
		return nil
	}

	ht := hit_tracker.NewHitTracker(ctx, s.env, false)
	digestsToInline := make([]*repb.Digest, 0, len(filesToInline))
	downloadTrackers := make([]*hit_tracker.TransferTimer, 0, len(filesToInline))
	for _, f := range filesToInline {
		digestsToInline = append(digestsToInline, f.GetDigest())
		downloadTrackers = append(downloadTrackers, ht.TrackDownload(f.GetDigest()))
	}
	brbRequest := &repb.BatchReadBlobsRequest{
		InstanceName:   req.GetInstanceName(),
		Digests:        digestsToInline,
		DigestFunction: req.GetDigestFunction(),
	}
	batchBlobs, err := s.env.GetContentAddressableStorageClient().BatchReadBlobs(ctx, brbRequest)
	if err != nil {
		return err
	}
	misses := []string{}
	hits := make(map[string]*repb.BatchReadBlobsResponse_Response)
	for _, r := range batchBlobs.Responses {
		// Make sure everything came back clean.
		if r.GetStatus().GetCode() != int32(codes.OK) {
			misses = append(misses, r.GetDigest().String())
		} else {
			hits[r.GetDigest().GetHash()] = r
		}
	}
	if len(misses) > 0 {
		// TODO(jdhollen): track misses?  This is literally an error case.
		return status.NotFoundErrorf("Not all requested CAS entries (%s) were found.", strings.Join(misses, ", "))
	}
	for i, f := range filesToInline {
		resp, ok := hits[f.GetDigest().GetHash()]
		if !ok {
			return status.InternalErrorf("BatchReadBlobs request was missing response for digest %v", f.GetDigest())
		}
		blob := resp.GetData()
		f.Contents = blob
		if err := downloadTrackers[i].CloseWithBytesTransferred(int64(len(blob)), int64(len(blob)), repb.Compressor_IDENTITY, "ac_proxy_server"); err != nil {
			log.Debugf("GetActionResult: download tracker error: %s", err)
		}
	}
	return nil
}

// Action Cache entries are not content-addressable, so the value pointed to
// by a given key may change in the backing cache. Thus, don't cache them
// locally when writing to the authoritative cache.
func (s *ActionCacheServerProxy) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest) (*repb.ActionResult, error) {
	resp, err := s.remoteCache.UpdateActionResult(ctx, req)
	labels := prometheus.Labels{
		metrics.StatusLabel:        fmt.Sprintf("%d", gstatus.Code(err)),
		metrics.CacheHitMissStatus: "miss",
	}
	metrics.ActionCacheProxiedWriteRequests.With(labels).Inc()
	metrics.ActionCacheProxiedWriteByes.With(labels).Add(float64(proto.Size(req)))
	return resp, err
}
