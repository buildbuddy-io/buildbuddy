package interceptors_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"runtime/pprof"
	rtrace "runtime/trace"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testport"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testpprof"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/pprofutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"github.com/google/pprof/profile"
	"github.com/stretchr/testify/require"
	exptrace "golang.org/x/exp/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"

	pspb "github.com/buildbuddy-io/buildbuddy/proto/ping_service"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

// handlerTimeout bounds waits for events that are guaranteed to happen, so that
// only a stalled machine hits it.
const handlerTimeout = 60 * time.Second

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
	// ended receives a value each time an RPC finishes, after the profiling
	// stats handler has reset the worker goroutine's labels.
	ended chan struct{}
}

// endNotifier is registered after ProfilingStatsHandler, so by the time it sees
// an RPC's End event that handler has already ended the RPC's trace task and
// reset the worker goroutine's labels. Unlike a response arriving at the
// client, which happens before the End event, this is a reliable sync point
// for tests that inspect state after an RPC.
type endNotifier struct{ ended chan struct{} }

func (e endNotifier) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (e endNotifier) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (e endNotifier) HandleConn(context.Context, stats.ConnStats) {}
func (e endNotifier) HandleRPC(_ context.Context, rs stats.RPCStats) {
	if _, ok := rs.(*stats.End); !ok {
		return
	}
	select {
	case e.ended <- struct{}{}:
	default: // Benchmarks don't wait for ends.
	}
}

// waitForEnds blocks until n more RPCs have fully finished on the server. Each
// is guaranteed to, so this only times out if the machine is stalled.
func waitForEnds(t testing.TB, s *labelServer, n int) {
	for range n {
		select {
		case <-s.ended:
		case <-time.After(handlerTimeout):
			require.FailNow(t, "RPC did not finish")
		}
	}
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
	s.ended = make(chan struct{}, 1000)
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1", "US2", "GR2")))
	grpcServer := grpc.NewServer(grpc_server.CommonGRPCServerOptionsWithConfig(env, grpc_server.GRPCServerConfig{
		ExtraStatsHandlers: []stats.Handler{endNotifier{s.ended}},
	})...)
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
	case <-time.After(handlerTimeout):
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

var burnSink int

// burnCPU does a fixed amount of CPU work, so the number of profile samples it
// yields depends on the CPU time it gets rather than on wall time.
//
//go:noinline
func burnCPU() {
	x := 0
	for i := range 100_000_000 {
		x += i * i
	}
	burnSink = x
}

// profileCPU runs f under the CPU profiler and returns the parsed profile.
func profileCPU(t *testing.T, f func()) *profile.Profile {
	var buf bytes.Buffer
	require.NoError(t, pprof.StartCPUProfile(&buf))
	// Make sure the profile is stopped even if f fails the test.
	var once sync.Once
	stop := func() { once.Do(pprof.StopCPUProfile) }
	defer stop()
	f()
	stop()
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

// maxProfileRuns bounds collectSamples. Each run yields samples in proportion
// to the CPU time f uses, whatever the load on the machine, so this is only
// reached if match never matches.
const maxProfileRuns = 200

// collectSamples repeatedly profiles f until at least minMatched samples
// satisfy match, and returns the total matched and labeled counts.
func collectSamples(t *testing.T, f func(), match func(funcNames []string) bool, want map[string]string, minMatched int) (matched, labeled int) {
	for run := 0; matched < minMatched; run++ {
		require.Less(t, run, maxProfileRuns, "only %d of %d samples matched", matched, minMatched)
		m, l := sampleStats(profileCPU(t, f), match, want)
		matched += m
		labeled += l
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
			burnCPU()
		}()
		<-done
	}
	c := startLabelServer(t, s)

	matched, labeled := collectSamples(t, func() {
		_, err := c.ping.Ping(requestCtx(t, "US1", "inv1"), &pspb.PingRequest{})
		require.NoError(t, err)
	},
		func(names []string) bool { return anyContains(names, ".burnCPU") },
		map[string]string{pprofutil.GRPCMethod: pingMethod, pprofutil.GroupID: "GR1"},
		10)
	// burnCPU only runs in the goroutine the handler starts, which inherits the
	// handler's labels, so every sample must carry them.
	require.Equal(t, matched, labeled, "labeled %d of %d samples", labeled, matched)
}

func TestCPUProfileLabelsUnaryDecodeAndEncode(t *testing.T) {
	enableLabels(t)
	s := &labelServer{bigDigests: 50000}
	c := startLabelServer(t, s)
	req := &repb.FindMissingBlobsRequest{BlobDigests: makeDigests(50000)}

	// Server-side marshal/unmarshal runs under (*Server).processRPC; the
	// client's does not. The match is narrowed to processRPC rather than
	// handleStream because handleStream also runs TagRPC (grpc v1.84
	// server.go:1578), which parses the request metadata before the labels are
	// set. processRPC (called at server.go:1595) starts only after TagRPC
	// returns, unary decode and encode run inside it, and its deferred End
	// handling (server.go:1330-1351) resets the labels without marshaling.
	matched, labeled := collectSamples(t, func() {
		_, err := c.cas.FindMissingBlobs(requestCtx(t, "US1", "inv1"), req)
		require.NoError(t, err)
	},
		func(names []string) bool {
			return anyContains(names, "(*Server).processRPC") && anyContains(names, "arshal")
		},
		map[string]string{pprofutil.GRPCMethod: findMethod},
		10)
	// The labels are set before processRPC starts and reset only at End, after
	// the response is encoded, so every sample under processRPC must carry the
	// method.
	require.Equal(t, matched, labeled, "labeled %d of %d samples", labeled, matched)
}

func TestIdleWorkersHaveNoLabels(t *testing.T) {
	enableLabels(t)
	before := testpprof.TakeSnapshot(t)
	s := &labelServer{}
	c := startLabelServer(t, s)

	for range 20 {
		_, err := c.ping.Ping(requestCtx(t, "US1", "inv1"), &pspb.PingRequest{})
		require.NoError(t, err)
		readStream(t, requestCtx(t, "US1", "inv1"), c.bs)
	}

	// The client gets its response before the server's End event, which is
	// when the labels are reset.
	waitForEnds(t, s, 40)

	// Only this test's server's workers: others may be shutting down.
	workers := before.GoroutineLabels(t, "(*Server).serverWorker(")
	require.NotEmpty(t, workers)
	for _, l := range workers {
		require.Empty(t, l)
	}
}

func BenchmarkInstrumentedServerPprofLabels(b *testing.B) {
	for _, tc := range []struct{ labels, tasks bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
		b.Run(fmt.Sprintf("labels=%v,tasks=%v", tc.labels, tc.tasks), func(b *testing.B) {
			flags.Set(b, "monitoring.enable_pprof_labels", tc.labels)
			flags.Set(b, "monitoring.enable_trace_tasks", tc.tasks)
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

// traceInfo summarizes the trace tasks of a given type found in an execution
// trace.
type traceInfo struct {
	begins, ends int
	// logs maps log category to messages, for logs attached to tasks of the type.
	logs map[string][]string
}

func readTraceTasks(t *testing.T, buf *bytes.Buffer, taskType string) traceInfo {
	r, err := exptrace.NewReader(buf)
	require.NoError(t, err)
	info := traceInfo{logs: map[string][]string{}}
	ids := map[exptrace.TaskID]bool{}
	for {
		ev, err := r.ReadEvent()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		switch ev.Kind() {
		case exptrace.EventTaskBegin:
			if ev.Task().Type == taskType {
				info.begins++
				ids[ev.Task().ID] = true
			}
		case exptrace.EventTaskEnd:
			if ev.Task().Type == taskType {
				info.ends++
			}
		case exptrace.EventLog:
			if l := ev.Log(); ids[l.Task] {
				info.logs[l.Category] = append(info.logs[l.Category], l.Message)
			}
		default:
		}
	}
	return info
}

func tracePing(t *testing.T) traceInfo {
	s := &labelServer{}
	c := startLabelServer(t, s)
	var buf bytes.Buffer
	require.NoError(t, rtrace.Start(&buf))
	// Make sure the trace is stopped even if the test fails.
	var once sync.Once
	stop := func() { once.Do(rtrace.Stop) }
	t.Cleanup(stop)
	_, err := c.ping.Ping(requestCtx(t, "US1", ""), &pspb.PingRequest{})
	// The task ends in the End stats event, which runs after the response is
	// sent.
	waitForEnds(t, s, 1)
	stop()
	require.NoError(t, err)
	return readTraceTasks(t, &buf, pingMethod)
}

func TestTraceTaskPerRequest(t *testing.T) {
	flags.Set(t, "monitoring.enable_trace_tasks", true)

	info := tracePing(t)

	require.Equal(t, 1, info.begins)
	require.Equal(t, 1, info.ends)
	require.Equal(t, []string{"GR1"}, info.logs[pprofutil.GroupID])
}

func TestTraceTaskWithLabelsEnabled(t *testing.T) {
	flags.Set(t, "monitoring.enable_trace_tasks", true)
	flags.Set(t, "monitoring.enable_pprof_labels", true)

	info := tracePing(t)

	require.Equal(t, 1, info.begins)
	require.Equal(t, 1, info.ends)
	require.Equal(t, []string{"GR1"}, info.logs[pprofutil.GroupID])
}

func TestNoTraceTaskWhenFlagOff(t *testing.T) {
	// Labels on, trace tasks off: the stats handler is registered but must not
	// create tasks.
	flags.Set(t, "monitoring.enable_pprof_labels", true)

	info := tracePing(t)

	require.Equal(t, 0, info.begins)
	require.Equal(t, 0, info.ends)
	require.Empty(t, info.logs)
}
