package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509/pkix"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/byte_stream_server_proxy"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/proxy_util"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/ssl"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	hlpb "google.golang.org/grpc/health/grpc_health_v1"
)

type shutdownHealthChecker struct {
	interfaces.HealthChecker
	shutdown interfaces.CheckerFunc
}

func (h *shutdownHealthChecker) RegisterShutdownFunction(f interfaces.CheckerFunc) { h.shutdown = f }

type shutdownListener struct {
	net.Listener
	closed chan struct{}
	once   sync.Once
}

func (l *shutdownListener) Close() error {
	err := l.Listener.Close()
	l.once.Do(func() { close(l.closed) })
	return err
}
func newShutdownListener(t *testing.T) *shutdownListener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { l.Close() })
	return &shutdownListener{Listener: l, closed: make(chan struct{})}
}
func testTLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	cert, key, err := ssl.GenerateCert(pkix.Name{CommonName: "localhost"}, nil, time.Hour)
	require.NoError(t, err)
	pair, err := tls.X509KeyPair([]byte(cert), []byte(key))
	require.NoError(t, err)
	return &tls.Config{Certificates: []tls.Certificate{pair}}
}
func awaitClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for shutdown")
	}
}
func requireListenerClosed(t *testing.T, l *shutdownListener) {
	t.Helper()
	awaitClosed(t, l.closed)
	conn, err := net.DialTimeout("tcp", l.Addr().String(), time.Second)
	if conn != nil {
		conn.Close()
	}
	require.Error(t, err, "draining server must reject new connections")
}

func TestHTTPSRequiresTLSConfig(t *testing.T) {
	hc := &shutdownHealthChecker{}
	env := real_environment.NewRealEnv(hc)
	listener := newShutdownListener(t)
	err := serveHTTP(env, &http.Server{}, listener, true)
	require.ErrorContains(t, err, "TLS is enabled but no TLS config is available")
	require.Nil(t, hc.shutdown, "invalid TLS configuration must be rejected before server startup")
}

func TestHTTPShutdown(t *testing.T) {
	for _, protocol := range []string{"http", "https"} {
		for _, mode := range []string{"drain", "deadline"} {
			t.Run(protocol+"/"+mode, func(t *testing.T) {
				release := make(chan struct{})
				defer close(release)
				finished := make(chan struct{})
				server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer close(finished)
					w.Write([]byte("first"))
					w.(http.Flusher).Flush()
					select {
					case <-release:
						w.Write([]byte("last"))
					case <-r.Context().Done():
					}
				})}
				defer server.Close()
				if protocol == "https" {
					server.TLSConfig = testTLSConfig(t)
				}
				hc := &shutdownHealthChecker{}
				env := real_environment.NewRealEnv(hc)
				listener := newShutdownListener(t)
				require.NoError(t, serveHTTP(env, server, listener, protocol == "https"))
				transport := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, ForceAttemptHTTP2: true}
				defer transport.CloseIdleConnections()
				client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
				response, err := client.Get(protocol + "://" + listener.Addr().String())
				require.NoError(t, err)
				defer response.Body.Close()
				if protocol == "https" {
					require.Equal(t, 2, response.ProtoMajor)
				}
				prefix := make([]byte, 5)
				_, err = io.ReadFull(response.Body, prefix)
				require.NoError(t, err)
				require.Equal(t, "first", string(prefix))
				timeout := 5 * time.Second
				if mode == "deadline" {
					timeout = 250 * time.Millisecond
				}
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				stopped := make(chan error, 1)
				require.NotNil(t, hc.shutdown)
				go func() { stopped <- hc.shutdown(ctx) }()
				requireListenerClosed(t, listener)
				if mode == "drain" {
					select {
					case err := <-stopped:
						t.Fatalf("shutdown returned before response finished: %v", err)
					default:
					}
					release <- struct{}{}
					body, err := io.ReadAll(response.Body)
					require.NoError(t, err)
					require.Equal(t, "last", string(body))
				} else {
					_, err := io.ReadAll(response.Body)
					require.Error(t, err, "deadline must close the unfinished response")
				}
				select {
				case err := <-stopped:
					if mode == "deadline" {
						require.ErrorIs(t, err, context.DeadlineExceeded)
					} else {
						require.NoError(t, err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("shutdown did not finish")
				}
				awaitClosed(t, finished)
				env.GetHTTPServerWaitGroup().Wait()
			})
		}
	}
}

type drainingHealthServer struct {
	hlpb.UnimplementedHealthServer
	release chan struct{}
}

func (s *drainingHealthServer) Watch(_ *hlpb.HealthCheckRequest, stream hlpb.Health_WatchServer) error {
	if err := stream.Send(&hlpb.HealthCheckResponse{Status: hlpb.HealthCheckResponse_SERVING}); err != nil {
		return err
	}
	select {
	case <-s.release:
		return stream.Send(&hlpb.HealthCheckResponse{Status: hlpb.HealthCheckResponse_NOT_SERVING})
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
}
func TestGRPCTLSShutdown(t *testing.T) {
	for _, mode := range []string{"drain", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			listener := newShutdownListener(t)
			server := grpc.NewServer(grpc.Creds(credentials.NewTLS(testTLSConfig(t))))
			defer server.Stop()
			handler := &drainingHealthServer{release: make(chan struct{})}
			defer close(handler.release)
			hlpb.RegisterHealthServer(server, handler)
			go server.Serve(listener)
			conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})))
			require.NoError(t, err)
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream, err := hlpb.NewHealthClient(conn).Watch(ctx, &hlpb.HealthCheckRequest{})
			require.NoError(t, err)
			first, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, hlpb.HealthCheckResponse_SERVING, first.Status)
			timeout := 5 * time.Second
			if mode == "deadline" {
				timeout = 350 * time.Millisecond
			}
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), timeout)
			defer shutdownCancel()
			stopped := make(chan error, 1)
			go func() { stopped <- grpc_server.GRPCShutdown(shutdownCtx, server) }()
			requireListenerClosed(t, listener)
			if mode == "drain" {
				select {
				case err := <-stopped:
					t.Fatalf("shutdown returned before stream finished: %v", err)
				default:
				}
				handler.release <- struct{}{}
				last, err := stream.Recv()
				require.NoError(t, err)
				require.Equal(t, hlpb.HealthCheckResponse_NOT_SERVING, last.Status)
				_, err = stream.Recv()
				require.ErrorIs(t, err, io.EOF)
			} else {
				_, err := stream.Recv()
				require.Error(t, err)
				require.NotErrorIs(t, err, io.EOF)
			}
			select {
			case err := <-stopped:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("shutdown did not finish")
			}
		})
	}
}

// Hold the first response in flight after delivering a message to the client.
type pausedReadStream struct {
	grpc.ServerStream
	release <-chan struct{}
	paused  bool
}

func (s *pausedReadStream) SendMsg(m any) error {
	if err := s.ServerStream.SendMsg(m); err != nil {
		return err
	}
	if !s.paused {
		s.paused = true
		select {
		case <-s.release:
		case <-s.Context().Done():
			return s.Context().Err()
		}
	}
	return nil
}

func TestByteStreamClientReconnectsDuringGracefulShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	firstListener := newShutdownListener(t)
	secondListener := newShutdownListener(t)
	var destination atomic.Value
	destination.Store(firstListener.Addr().String())
	var dialCount atomic.Int32
	// Model a stable service address whose new connections go to a ready pod.
	// This selects a TCP destination only; each cache proxy terminates TLS itself.
	conn, err := grpc.NewClient("passthrough:///cache-proxy", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		dialCount.Add(1)
		return (&net.Dialer{}).DialContext(ctx, "tcp", destination.Load().(string))
	}), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})))
	require.NoError(t, err)
	defer conn.Close()
	client := bspb.NewByteStreamClient(conn)
	data := bytes.Repeat([]byte("cache-proxy-shutdown"), 128*1024)
	d, err := digest.Compute(bytes.NewReader(data), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	resource := digest.NewCASResourceName(d, "", repb.DigestFunction_SHA256)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	startProxy := func(name string, listener net.Listener, pause bool) *grpc.Server {
		env := testenv.GetTestEnv(t)
		cacheCtx, err := prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator())
		require.NoError(t, err)
		require.NoError(t, env.GetCache().Set(cacheCtx, resource.ToProto(), data))
		local, err := byte_stream_server.NewByteStreamServer(env)
		require.NoError(t, err)
		env.SetLocalByteStreamServer(local)
		env.SetByteStreamClient(client) // Reads below explicitly use the local cache.
		proxy, err := byte_stream_server_proxy.New(env)
		require.NoError(t, err)
		var paused atomic.Bool
		server := grpc.NewServer(grpc.Creds(credentials.NewTLS(testTLSConfig(t))), grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if err := stream.SetHeader(metadata.Pairs("test-endpoint", name)); err != nil {
				return err
			}
			if pause && paused.CompareAndSwap(false, true) {
				stream = &pausedReadStream{ServerStream: stream, release: release}
			}
			return handler(srv, stream)
		}))
		bspb.RegisterByteStreamServer(server, proxy)
		t.Cleanup(server.Stop)
		go server.Serve(listener)
		return server
	}
	firstServer := startProxy("first", firstListener, true)
	startProxy("second", secondListener, false)
	readCtx := proxy_util.SetSkipRemote(ctx)
	firstRead, err := client.Read(readCtx, &bspb.ReadRequest{ResourceName: resource.DownloadString()})
	require.NoError(t, err)
	firstChunk, err := firstRead.Recv()
	require.NoError(t, err)
	headers, err := firstRead.Header()
	require.NoError(t, err)
	require.Equal(t, []string{"first"}, headers.Get("test-endpoint"))
	require.Less(t, len(firstChunk.Data), len(data))
	require.EqualValues(t, 1, dialCount.Load())

	destination.Store(secondListener.Addr().String())
	stopped := make(chan error, 1)
	go func() { stopped <- grpc_server.GRPCShutdown(ctx, firstServer) }()
	awaitClosed(t, firstListener.closed)
	// Listener closure precedes GOAWAY delivery. Wait until the client stops
	// treating the old transport as ready before starting the next read.
	require.True(t, conn.WaitForStateChange(ctx, connectivity.Ready), "client did not observe the draining transport")
	// No Close, Connect, resolver update, or application retry: GOAWAY must
	// cause this same ClientConn to establish a new connection for the next RPC.
	secondRead, err := client.Read(readCtx, &bspb.ReadRequest{ResourceName: resource.DownloadString()})
	require.NoError(t, err)
	receive := func(stream bspb.ByteStream_ReadClient, initial []byte) []byte {
		result := append([]byte(nil), initial...)
		for {
			response, err := stream.Recv()
			if err == io.EOF {
				return result
			}
			require.NoError(t, err)
			result = append(result, response.Data...)
		}
	}
	require.Equal(t, data, receive(secondRead, nil))
	headers, err = secondRead.Header()
	require.NoError(t, err)
	require.Equal(t, []string{"second"}, headers.Get("test-endpoint"))
	require.GreaterOrEqual(t, dialCount.Load(), int32(2))
	select {
	case err := <-stopped:
		t.Fatalf("first proxy stopped before its active read finished: %v", err)
	default:
	}
	unblock()
	require.Equal(t, data, receive(firstRead, firstChunk.Data))
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("first proxy did not finish draining")
	}
}
