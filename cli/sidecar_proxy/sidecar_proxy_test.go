package sidecar_proxy_test

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/sidecar_proxy"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/capabilities_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

// harness wires the sidecar proxy in front of an in-process "remote" gRPC
// cache. RPCs the sidecar proxy issues to the remote are recorded by a
// server-side interceptor on the remote.
type harness struct {
	// sidecarConn is the client connection to the sidecar proxy.
	sidecarConn *grpc.ClientConn
	// remoteConn is a direct connection to the remote, for out-of-band setup.
	remoteConn *grpc.ClientConn
	// remote records inbound RPCs at the remote.
	remote *recordingRemote
}

// harnessOpts controls which gRPC services are registered on the in-process
// remote. By default all of them are; tests that want to exercise the
// proxy's behavior when the remote is missing a service can opt out.
type harnessOpts struct {
	skipRemoteCapabilities bool
}

func newHarness(t *testing.T) *harness {
	return newHarnessWithOpts(t, harnessOpts{})
}

func newHarnessWithOpts(t *testing.T, opts harnessOpts) *harness {
	t.Helper()
	// The proxy's internal local bytestream server runs for the lifetime of
	// this context.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	remoteEnv := testenv.GetTestEnv(t)
	remote := &recordingRemote{counts: map[string]int64{}}
	remoteSrv, remoteLis := newRemoteServer(t, remoteEnv, remote)
	bsServer, err := byte_stream_server.NewByteStreamServer(remoteEnv)
	require.NoError(t, err)
	casServer, err := content_addressable_storage_server.NewContentAddressableStorageServer(remoteEnv)
	require.NoError(t, err)
	acServer, err := action_cache_server.NewActionCacheServer(remoteEnv)
	require.NoError(t, err)
	bspb.RegisterByteStreamServer(remoteSrv, bsServer)
	repb.RegisterContentAddressableStorageServer(remoteSrv, casServer)
	repb.RegisterActionCacheServer(remoteSrv, acServer)
	if !opts.skipRemoteCapabilities {
		capServer := capabilities_server.NewCapabilitiesServer(remoteEnv, true /*supportCAS*/, false /*supportRemoteExec*/, true /*supportZstd*/)
		repb.RegisterCapabilitiesServer(remoteSrv, capServer)
	}
	go func() { _ = remoteSrv.Serve(remoteLis) }()
	t.Cleanup(remoteSrv.Stop)

	remoteConn, err := dialBufconn(ctx, remoteLis)
	require.NoError(t, err)
	t.Cleanup(func() { _ = remoteConn.Close() })

	localEnv := testenv.GetTestEnv(t)
	proxy, err := sidecar_proxy.NewCacheProxy(ctx, localEnv, remoteConn)
	require.NoError(t, err)

	sidecarSrv, sidecarLis := newSidecarServer(t, localEnv)
	bspb.RegisterByteStreamServer(sidecarSrv, proxy)
	repb.RegisterContentAddressableStorageServer(sidecarSrv, proxy)
	repb.RegisterActionCacheServer(sidecarSrv, proxy)
	repb.RegisterCapabilitiesServer(sidecarSrv, proxy)
	go func() { _ = sidecarSrv.Serve(sidecarLis) }()
	t.Cleanup(sidecarSrv.Stop)

	sidecarConn, err := dialBufconn(ctx, sidecarLis)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sidecarConn.Close() })

	return &harness{
		sidecarConn: sidecarConn,
		remoteConn:  remoteConn,
		remote:      remote,
	}
}

// recordingRemote records each inbound RPC at the remote: method name,
// observed inbound metadata, and a per-method counter.
type recordingRemote struct {
	mu     sync.Mutex
	counts map[string]int64
	mds    []recordedMD
}

type recordedMD struct {
	method string
	md     metadata.MD
}

func (r *recordingRemote) record(method string, md metadata.MD) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counts[method]++
	r.mds = append(r.mds, recordedMD{method: method, md: md.Copy()})
}

func (r *recordingRemote) count(method string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[method]
}

// metadataForMethod returns all observed inbound metadata for RPCs whose full
// method name is suffixed by `methodSuffix`. Useful for asserting "what did
// the remote see for Write calls" without depending on the full proto path.
func (r *recordingRemote) metadataForMethod(methodSuffix string) []metadata.MD {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []metadata.MD
	for _, e := range r.mds {
		if strings.HasSuffix(e.method, methodSuffix) {
			out = append(out, e.md)
		}
	}
	return out
}

func (r *recordingRemote) unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	r.record(info.FullMethod, md)
	return handler(ctx, req)
}

func (r *recordingRemote) stream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	md, _ := metadata.FromIncomingContext(ss.Context())
	r.record(info.FullMethod, md)
	return handler(srv, ss)
}

func newRemoteServer(t *testing.T, env *real_environment.RealEnv, rec *recordingRemote) (*grpc.Server, *bufconn.Listener) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024 * 4)
	opts := append([]grpc.ServerOption{}, grpc_server.CommonGRPCServerOptions(env)...)
	opts = append(opts,
		grpc.ChainUnaryInterceptor(rec.unary),
		grpc.ChainStreamInterceptor(rec.stream),
	)
	return grpc.NewServer(opts...), lis
}

func newSidecarServer(t *testing.T, env *real_environment.RealEnv) (*grpc.Server, *bufconn.Listener) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024 * 4)
	opts := grpc_server.CommonGRPCServerOptions(env)
	return grpc.NewServer(opts...), lis
}

func dialBufconn(ctx context.Context, lis *bufconn.Listener) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4*1024*1024)),
	)
}

// uploadBlob writes a blob through the given byte stream client and returns
// the corresponding CAS resource name.
func uploadBlob(t *testing.T, ctx context.Context, bsClient bspb.ByteStreamClient, data []byte) *digest.CASResourceName {
	t.Helper()
	d, err := digest.Compute(bytes.NewReader(data), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	rn := digest.NewCASResourceName(d, "" /*instanceName*/, repb.DigestFunction_SHA256)
	_, _, err = cachetools.UploadFromReader(ctx, bsClient, rn, bytes.NewReader(data))
	require.NoError(t, err)
	return rn
}

// readBlob fetches a blob via the given byte stream client and returns its
// bytes.
func readBlob(t *testing.T, ctx context.Context, bsClient bspb.ByteStreamClient, rn *digest.CASResourceName) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, cachetools.GetBlob(ctx, bsClient, rn, &buf))
	return buf.Bytes()
}

// TestRead_HitsRemoteOnceThenLocal verifies the read-through contract: a blob
// served through the sidecar proxy is fetched from the remote on the first
// read and served from the local cache on subsequent reads.
func TestRead_HitsRemoteOnceThenLocal(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	// Seed the blob directly on the remote, bypassing the sidecar proxy.
	const payload = "hello from the remote"
	rn := uploadBlob(t, ctx, bspb.NewByteStreamClient(h.remoteConn), []byte(payload))

	remoteWritesAfterSeed := h.remote.count("/google.bytestream.ByteStream/Write")

	// Read once via the sidecar — this should hit the remote and populate the
	// local cache.
	sidecarBS := bspb.NewByteStreamClient(h.sidecarConn)
	got1 := readBlob(t, ctx, sidecarBS, rn)
	require.Equal(t, payload, string(got1))
	require.Equal(t, int64(1), h.remote.count("/google.bytestream.ByteStream/Read"),
		"first read should reach the remote exactly once")
	require.Equal(t, remoteWritesAfterSeed, h.remote.count("/google.bytestream.ByteStream/Write"),
		"first read must not write back to the remote")

	// Read again — this should be served entirely from the local cache and
	// must not touch the remote.
	got2 := readBlob(t, ctx, sidecarBS, rn)
	require.Equal(t, payload, string(got2))
	require.Equal(t, int64(1), h.remote.count("/google.bytestream.ByteStream/Read"),
		"second read should be served locally and not hit the remote")
}

// TestWrite_AsyncPropagatesToRemote verifies that with the default
// configuration, a write through the sidecar proxy returns success
// immediately and the blob eventually appears on the remote.
func TestWrite_AsyncPropagatesToRemote(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	d, reader := testdigest.NewReader(t, 1024)
	rn := digest.NewCASResourceName(d, "" /*instanceName*/, repb.DigestFunction_SHA256)

	sidecarBS := bspb.NewByteStreamClient(h.sidecarConn)
	_, _, err := cachetools.UploadFromReader(ctx, sidecarBS, rn, reader)
	require.NoError(t, err)

	// The blob may not be on the remote yet — the upload is queued. Poll the
	// remote directly until it shows up.
	remoteBS := bspb.NewByteStreamClient(h.remoteConn)
	require.Eventually(t, func() bool {
		var buf bytes.Buffer
		err := cachetools.GetBlob(ctx, remoteBS, rn, &buf)
		return err == nil && int64(buf.Len()) == d.GetSizeBytes()
	}, 5*time.Second, 10*time.Millisecond, "blob did not propagate to remote")
}

// TestWrite_SyncBlocksUntilRemoteHasBlob verifies that with
// --local_cache_proxy.synchronous_write set, a write through the sidecar
// proxy does not return until the remote has the blob.
func TestWrite_SyncBlocksUntilRemoteHasBlob(t *testing.T) {
	flags.Set(t, "local_cache_proxy.synchronous_write", true)

	h := newHarness(t)
	ctx := context.Background()

	d, reader := testdigest.NewReader(t, 1024)
	rn := digest.NewCASResourceName(d, "" /*instanceName*/, repb.DigestFunction_SHA256)

	sidecarBS := bspb.NewByteStreamClient(h.sidecarConn)
	_, _, err := cachetools.UploadFromReader(ctx, sidecarBS, rn, reader)
	require.NoError(t, err)

	// Synchronous write means the remote must already have the blob by the
	// time the client write returns. No polling.
	remoteBS := bspb.NewByteStreamClient(h.remoteConn)
	var buf bytes.Buffer
	require.NoError(t, cachetools.GetBlob(ctx, remoteBS, rn, &buf))
	require.Equal(t, d.GetSizeBytes(), int64(buf.Len()))
}

// TestWrite_PropagatesMetadataAsync verifies that the API key header and the
// bazel RequestMetadata reach the remote on the asynchronous write path. The
// async path is the trickier one because metadata is captured at Write time
// and replayed by a background worker — easy to break.
func TestWrite_PropagatesMetadataAsync(t *testing.T) {
	h := newHarness(t)
	ctx := newCtxWithMetadata(t, "test-api-key", "inv-async")

	d, reader := testdigest.NewReader(t, 1024)
	rn := digest.NewCASResourceName(d, "", repb.DigestFunction_SHA256)

	sidecarBS := bspb.NewByteStreamClient(h.sidecarConn)
	_, _, err := cachetools.UploadFromReader(ctx, sidecarBS, rn, reader)
	require.NoError(t, err)

	// Wait for the queued upload to reach the remote, then check the metadata
	// it arrived with.
	require.Eventually(t, func() bool {
		for _, md := range h.remote.metadataForMethod("/Write") {
			if mdHasAPIKey(md, "test-api-key") && mdHasInvocationID(md, "inv-async") {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "remote never saw the expected metadata on a Write")
}

// TestWrite_PropagatesMetadataSync verifies API key + RequestMetadata reach
// the remote on the synchronous write path.
func TestWrite_PropagatesMetadataSync(t *testing.T) {
	flags.Set(t, "local_cache_proxy.synchronous_write", true)

	h := newHarness(t)
	ctx := newCtxWithMetadata(t, "test-api-key", "inv-sync")

	d, reader := testdigest.NewReader(t, 1024)
	rn := digest.NewCASResourceName(d, "", repb.DigestFunction_SHA256)

	sidecarBS := bspb.NewByteStreamClient(h.sidecarConn)
	_, _, err := cachetools.UploadFromReader(ctx, sidecarBS, rn, reader)
	require.NoError(t, err)

	// On the sync path the upload must have reached the remote already; check
	// metadata without polling.
	var saw bool
	for _, md := range h.remote.metadataForMethod("/Write") {
		if mdHasAPIKey(md, "test-api-key") && mdHasInvocationID(md, "inv-sync") {
			saw = true
			break
		}
	}
	require.True(t, saw, "remote never saw the expected metadata on the sync Write path")
}

// TestFindMissingBlobs_ForwardedToRemote verifies the proxy forwards
// FindMissingBlobs straight through to the remote (no local short-circuit
// for this RPC). We populate the remote with one blob and ask about two; the
// remote's answer must come through unchanged.
func TestFindMissingBlobs_ForwardedToRemote(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	// Seed one blob on the remote.
	presentRN := uploadBlob(t, ctx, bspb.NewByteStreamClient(h.remoteConn), []byte("present"))

	// Construct a digest that's deliberately *not* on the remote.
	absentRN := digest.NewCASResourceName(
		&repb.Digest{
			Hash:      strings.Repeat("0", 64),
			SizeBytes: 7,
		},
		"" /*instanceName*/, repb.DigestFunction_SHA256,
	)

	before := h.remote.count("/build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs")

	cas := repb.NewContentAddressableStorageClient(h.sidecarConn)
	rsp, err := cas.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
		BlobDigests: []*repb.Digest{presentRN.GetDigest(), absentRN.GetDigest()},
	})
	require.NoError(t, err)

	require.Equal(t, before+1, h.remote.count("/build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs"),
		"FindMissingBlobs must be forwarded to the remote")
	require.Len(t, rsp.GetMissingBlobDigests(), 1, "exactly one blob should be reported missing")
	require.Equal(t, absentRN.GetDigest().GetHash(), rsp.GetMissingBlobDigests()[0].GetHash())
}

// TestGetCapabilities_FallsBackOnRemoteError verifies that when the remote
// fails to serve GetCapabilities (here: the service isn't registered, so the
// remote returns Unimplemented), the proxy still returns a valid
// ServerCapabilities response so the build can proceed.
func TestGetCapabilities_FallsBackOnRemoteError(t *testing.T) {
	h := newHarnessWithOpts(t, harnessOpts{skipRemoteCapabilities: true})
	ctx := context.Background()

	cap := repb.NewCapabilitiesClient(h.sidecarConn)
	resp, err := cap.GetCapabilities(ctx, &repb.GetCapabilitiesRequest{})
	require.NoError(t, err, "proxy must mask remote Capabilities failures")
	require.NotNil(t, resp.GetCacheCapabilities(), "fallback capabilities must advertise cache support")
	require.Contains(t, resp.GetCacheCapabilities().GetDigestFunctions(), repb.DigestFunction_SHA256,
		"fallback capabilities must advertise SHA256")
}

func newCtxWithMetadata(t *testing.T, apiKey, invocationID string) context.Context {
	t.Helper()
	rmd := &repb.RequestMetadata{ToolInvocationId: invocationID}
	b, err := proto.Marshal(rmd)
	require.NoError(t, err)
	return metadata.AppendToOutgoingContext(context.Background(),
		authutil.APIKeyHeader, apiKey,
		bazel_request.RequestMetadataKey, string(b),
	)
}

func mdHasAPIKey(md metadata.MD, want string) bool {
	for _, v := range md.Get(authutil.APIKeyHeader) {
		if v == want {
			return true
		}
	}
	return false
}

func mdHasInvocationID(md metadata.MD, want string) bool {
	for _, raw := range md.Get(bazel_request.RequestMetadataKey) {
		rmd := &repb.RequestMetadata{}
		if err := proto.Unmarshal([]byte(raw), rmd); err != nil {
			continue
		}
		if rmd.GetToolInvocationId() == want {
			return true
		}
	}
	return false
}
