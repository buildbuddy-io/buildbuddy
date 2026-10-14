package interceptors_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/pprofutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/pprofutil/pprofutiltest"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"github.com/google/pprof/profile"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pspb "github.com/buildbuddy-io/buildbuddy/proto/ping_service"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	pingMethod = "/ping.Api/Ping"
	readMethod = "/google.bytestream.ByteStream/Read"
	findMethod = "/build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs"
)

var labelKeys = []string{
	pprofutil.GRPCMethod, pprofutil.Client, pprofutil.Origin,
	pprofutil.InvocationID, pprofutil.ActionMnemonic, pprofutil.GroupID,
}

// labelsOf returns the pprof labels carried by ctx.
func labelsOf(ctx context.Context) map[string]string {
	m := map[string]string{}
	for _, k := range labelKeys {
		if v, ok := pprof.Label(ctx, k); ok {
			m[k] = v
		}
	}
	return m
}

// labelServer implements the services used by these tests, recording the
// labels each handler's ctx carries.
type labelServer struct {
	pspb.UnimplementedApiServer
	bspb.UnimplementedByteStreamServer
	repb.UnimplementedContentAddressableStorageServer

	// onPing, if set, runs in the Ping handler.
	onPing func(ctx context.Context)
	// bigDigests is the number of digests FindMissingBlobs returns.
	bigDigests int
	labels     chan map[string]string
}

func (s *labelServer) record(ctx context.Context) {
	select {
	case s.labels <- labelsOf(ctx):
	default:
	}
}

func (s *labelServer) Ping(ctx context.Context, req *pspb.PingRequest) (*pspb.PingResponse, error) {
	s.record(ctx)
	if s.onPing != nil {
		s.onPing(ctx)
	}
	return &pspb.PingResponse{Tag: req.GetTag()}, nil
}

func (s *labelServer) Read(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	s.record(stream.Context())
	return stream.Send(&bspb.ReadResponse{Data: []byte("x")})
}

func (s *labelServer) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	s.record(ctx)
	return &repb.FindMissingBlobsResponse{MissingBlobDigests: makeDigests(s.bigDigests)}, nil
}

func makeDigests(n int) []*repb.Digest {
	ds := make([]*repb.Digest, n)
	for i := range ds {
		ds[i] = &repb.Digest{Hash: fmt.Sprintf("%064d", i), SizeBytes: int64(i)}
	}
	return ds
}

type testClients struct {
	ping pspb.ApiClient
	bs   bspb.ByteStreamClient
	cas  repb.ContentAddressableStorageClient
}

// startLabelServer starts a server with the full interceptor chain. Set the
// pprof flag before calling.
func startLabelServer(t testing.TB, s *labelServer) testClients {
	s.labels = make(chan map[string]string, 1000)
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1", "US2", "GR2")))
	grpcServer := grpc.NewServer(grpc_server.CommonGRPCServerOptions(env)...)
	pspb.RegisterApiServer(grpcServer, s)
	bspb.RegisterByteStreamServer(grpcServer, s)
	repb.RegisterContentAddressableStorageServer(grpcServer, s)

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", testport.FindFree(t)))
	require.NoError(t, err)
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1<<30)))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return testClients{
		ping: pspb.NewApiClient(conn),
		bs:   bspb.NewByteStreamClient(conn),
		cas:  repb.NewContentAddressableStorageClient(conn),
	}
}

// requestCtx returns an outgoing context with the given user's API key (if
// any) and Bazel request metadata.
func requestCtx(t testing.TB, apiKey string, invocationID string, extra ...string) context.Context {
	ctx := context.Background()
	if apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, authutil.APIKeyHeader, apiKey)
	}
	rmd, err := proto.Marshal(&repb.RequestMetadata{
		ToolInvocationId: invocationID,
		ActionMnemonic:   "GoCompile",
	})
	require.NoError(t, err)
	ctx = metadata.AppendToOutgoingContext(ctx, bazel_request.RequestMetadataKey, string(rmd))
	if len(extra) > 0 {
		ctx = metadata.AppendToOutgoingContext(ctx, extra...)
	}
	return ctx
}

func next(t *testing.T, s *labelServer) map[string]string {
	select {
	case l := <-s.labels:
		return l
	case <-time.After(10 * time.Second):
		require.FailNow(t, "handler did not run")
		return nil
	}
}

func readStream(t *testing.T, ctx context.Context, c bspb.ByteStreamClient) {
	stream, err := c.Read(ctx, &bspb.ReadRequest{ResourceName: "x"})
	require.NoError(t, err)
	for {
		if _, err := stream.Recv(); err != nil {
			require.Equal(t, io.EOF, err)
			return
		}
	}
}

func enableLabels(t *testing.T) {
	flags.Set(t, "monitoring.enable_pprof_labels", true)
}

func TestUnaryRequestLabels(t *testing.T) {
	enableLabels(t)
	s := &labelServer{}
	c := startLabelServer(t, s)

	ctx := requestCtx(t, "US1", "inv1", usageutil.ClientHeaderName, "ios", usageutil.OriginHeaderName, "web")
	_, err := c.ping.Ping(ctx, &pspb.PingRequest{})
	require.NoError(t, err)

	require.Equal(t, map[string]string{
		pprofutil.GRPCMethod:     pingMethod,
		pprofutil.Client:         "ios",
		pprofutil.Origin:         "web",
		pprofutil.InvocationID:   "inv1",
		pprofutil.ActionMnemonic: "GoCompile",
		pprofutil.GroupID:        "GR1",
	}, next(t, s))
}

func TestStreamRequestLabels(t *testing.T) {
	enableLabels(t)
	s := &labelServer{}
	c := startLabelServer(t, s)

	ctx := requestCtx(t, "US1", "inv1", usageutil.ClientHeaderName, "ios", usageutil.OriginHeaderName, "web")
	readStream(t, ctx, c.bs)

	require.Equal(t, map[string]string{
		pprofutil.GRPCMethod:     readMethod,
		pprofutil.Client:         "ios",
		pprofutil.Origin:         "web",
		pprofutil.InvocationID:   "inv1",
		pprofutil.ActionMnemonic: "GoCompile",
		pprofutil.GroupID:        "GR1",
	}, next(t, s))
}

func TestUnauthenticatedRequestLabels(t *testing.T) {
	enableLabels(t)
	s := &labelServer{}
	c := startLabelServer(t, s)

	_, err := c.ping.Ping(requestCtx(t, "", "inv1"), &pspb.PingRequest{})
	require.NoError(t, err)

	require.Equal(t, map[string]string{
		pprofutil.GRPCMethod:     pingMethod,
		pprofutil.InvocationID:   "inv1",
		pprofutil.ActionMnemonic: "GoCompile",
	}, next(t, s))
}

func TestNoLabelsWhenFlagOff(t *testing.T) {
	s := &labelServer{}
	c := startLabelServer(t, s)

	_, err := c.ping.Ping(requestCtx(t, "US1", "inv1"), &pspb.PingRequest{})
	require.NoError(t, err)
	readStream(t, requestCtx(t, "US1", "inv1"), c.bs)

	require.Empty(t, next(t, s))
	require.Empty(t, next(t, s))
}

func TestLabelsDoNotLeakAcrossRequests(t *testing.T) {
	enableLabels(t)
	s := &labelServer{}
	c := startLabelServer(t, s)

	for i := range 50 {
		if i%2 == 0 {
			_, err := c.ping.Ping(requestCtx(t, "US1", ""), &pspb.PingRequest{})
			require.NoError(t, err)
			l := next(t, s)
			require.Equal(t, pingMethod, l[pprofutil.GRPCMethod])
			require.Equal(t, "GR1", l[pprofutil.GroupID])
		} else {
			readStream(t, requestCtx(t, "US2", ""), c.bs)
			l := next(t, s)
			require.Equal(t, readMethod, l[pprofutil.GRPCMethod])
			require.Equal(t, "GR2", l[pprofutil.GroupID])
		}
	}
}

var burnSink int

// burnCPU spins for d.
func burnCPU(d time.Duration) {
	end := time.Now().Add(d)
	x := 0
	for time.Now().Before(end) {
		for i := range 10000 {
			x += i * i
		}
	}
	burnSink = x
}

// profileCPU runs f under the CPU profiler and returns the parsed profile.
func profileCPU(t *testing.T, f func()) *profile.Profile {
	var buf bytes.Buffer
	// Sample faster than the default 100Hz to get enough samples quickly. This
	// must be set before starting the profile.
	runtime.SetCPUProfileRate(1000)
	require.NoError(t, pprof.StartCPUProfile(&buf))
	f()
	pprof.StopCPUProfile()
	p, err := profile.Parse(&buf)
	require.NoError(t, err)
	return p
}

// sampleStats returns the number of samples whose stack satisfies match, and
// how many of those carry all of the wanted labels.
func sampleStats(p *profile.Profile, match func(funcNames []string) bool, want map[string]string) (matched, labeled int) {
	for _, s := range p.Sample {
		var names []string
		for _, loc := range s.Location {
			for _, line := range loc.Line {
				names = append(names, line.Function.Name)
			}
		}
		if !match(names) {
			continue
		}
		matched++
		ok := true
		for k, v := range want {
			if len(s.Label[k]) != 1 || s.Label[k][0] != v {
				ok = false
			}
		}
		if ok {
			labeled++
		}
	}
	return matched, labeled
}

func anyContains(names []string, substr string) bool {
	for _, n := range names {
		if strings.Contains(n, substr) {
			return true
		}
	}
	return false
}

func TestCPUProfileLabelsHandlerWork(t *testing.T) {
	enableLabels(t)
	s := &labelServer{}
	// Burn in a child goroutine, which must inherit the handler's labels.
	s.onPing = func(ctx context.Context) {
		done := make(chan struct{})
		go func() {
			defer close(done)
			burnCPU(time.Second)
		}()
		<-done
	}
	c := startLabelServer(t, s)

	p := profileCPU(t, func() {
		_, err := c.ping.Ping(requestCtx(t, "US1", "inv1"), &pspb.PingRequest{})
		require.NoError(t, err)
	})

	matched, labeled := sampleStats(p,
		func(names []string) bool { return anyContains(names, ".burnCPU") },
		map[string]string{pprofutil.GRPCMethod: pingMethod, pprofutil.GroupID: "GR1"})
	require.GreaterOrEqual(t, matched, 3, "too few burn samples")
	require.GreaterOrEqual(t, float64(labeled)/float64(matched), 0.9, "labeled %d of %d samples", labeled, matched)
}

func TestCPUProfileLabelsUnaryDecodeAndEncode(t *testing.T) {
	enableLabels(t)
	s := &labelServer{bigDigests: 50000}
	c := startLabelServer(t, s)
	req := &repb.FindMissingBlobsRequest{BlobDigests: makeDigests(50000)}

	p := profileCPU(t, func() {
		end := time.Now().Add(time.Second)
		for time.Now().Before(end) {
			_, err := c.cas.FindMissingBlobs(requestCtx(t, "US1", "inv1"), req)
			require.NoError(t, err)
		}
	})

	// Server-side marshal/unmarshal runs under handleStream; the client's
	// does not.
	matched, labeled := sampleStats(p,
		func(names []string) bool {
			return anyContains(names, "(*Server).handleStream") && anyContains(names, "arshal")
		},
		map[string]string{pprofutil.GRPCMethod: findMethod})
	require.GreaterOrEqual(t, matched, 5, "too few encode/decode samples")
	require.GreaterOrEqual(t, float64(labeled)/float64(matched), 0.8, "labeled %d of %d samples", labeled, matched)
}

func TestIdleWorkersHaveNoLabels(t *testing.T) {
	enableLabels(t)
	s := &labelServer{}
	c := startLabelServer(t, s)

	for range 20 {
		_, err := c.ping.Ping(requestCtx(t, "US1", "inv1"), &pspb.PingRequest{})
		require.NoError(t, err)
		readStream(t, requestCtx(t, "US1", "inv1"), c.bs)
	}

	require.Eventually(t, func() bool {
		sets := pprofutiltest.GoroutineLabels(t, "serverWorker")
		if len(sets) == 0 {
			return false
		}
		for _, labels := range sets {
			if _, ok := labels[pprofutil.GRPCMethod]; ok {
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)
}

func BenchmarkInstrumentedServerPprofLabels(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		b.Run(fmt.Sprintf("labels=%v", enabled), func(b *testing.B) {
			flags.Set(b, "monitoring.enable_pprof_labels", enabled)
			c := startLabelServer(b, &labelServer{})
			ctx := requestCtx(b, "US1", "inv1", usageutil.ClientHeaderName, "ios")
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				if _, err := c.ping.Ping(ctx, &pspb.PingRequest{Tag: uint64(n)}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
