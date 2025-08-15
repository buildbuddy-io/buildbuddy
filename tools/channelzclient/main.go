package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	czpb "github.com/buildbuddy-io/buildbuddy/proto/channelz"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"golang.org/x/sync/errgroup"
)

var (
	env        = flag.String("env", "dev", "Environment, dev or prod.")
	target     = flag.String("target", "grpc://localhost:1987", "target host:port")
	serverName = flag.String("server_name", "", "server name")
	app        = flag.String("app", "all", "the app to collect channelz data for. By default collects for all apps. To collect for one app, should look something like 'buildbuddy-app-8'")
	parallel   = flag.Int("p", 50, "paralellism when fetching socket information")
	outDir     = flag.String("out", "/tmp/channelz", "output directory - should be an absolute path, so it isn't written to bazel's workspace root")
)

type socketData struct {
	peer string
	data *czpb.SocketData
}

func main() {
	flag.Parse()
	ctx := context.Background()

	targets, err := getTargets()
	if err != nil {
		log.Fatalf("%s", err)
	}

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	for _, t := range targets {
		if err := t.StartPortForwarding(ctx); err != nil {
			log.Fatalf("%s", err)
		}
		addr := fmt.Sprintf("grpc://localhost:%d", t.LocalPort)
		conn, err := grpc_client.DialSimple(addr)
		if err != nil {
			log.Fatalf("Error creating grpc client: %s", err)
		}

		if err := os.MkdirAll(*outDir, 0755); err != nil {
			log.Fatalf("%s", err)
		}
		file := filepath.Join(*outDir, fmt.Sprintf("%s.out", t.Pod))
		out, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			log.Fatalf("failed to open output file: %v", err)
		}
		defer out.Close()

		writer := bufio.NewWriter(out)
		defer writer.Flush()

		c := czpb.NewChannelzClient(conn)

		serverID := int64(0)
		if *serverName != "" {
			log.Infof("Looking up server %q", *serverName)
			rsp, err := c.GetServers(ctx, &czpb.GetServersRequest{})
			if err != nil {
				log.Fatalf("Error getting servers: %s", err)
			}
			for _, s := range rsp.Server {
				for _, ls := range s.ListenSocket {
					if ls.GetName() == *serverName {
						serverID = s.GetRef().GetServerId()
						break
					}
				}
				if serverID != 0 {
					break
				}
			}
			if serverID == 0 {
				log.Fatalf("Could not find server %q", *serverName)
			}
		}
		if serverID != 0 {
			log.Infof(fmt.Sprintf("Getting server sockets for %d", serverID))
			startSocketID := int64(0)
			var socketRefs []*czpb.SocketRef
			for {
				log.Infof(fmt.Sprintf("Getting server sockets starting with %d", startSocketID))
				rsp, err := c.GetServerSockets(ctx, &czpb.GetServerSocketsRequest{ServerId: serverID, StartSocketId: startSocketID, MaxResults: 1000})
				if err != nil {
					log.Fatalf("Error getting server sockets: %s", err)
				}
				socketRefs = append(socketRefs, rsp.GetSocketRef()...)
				if rsp.GetEnd() {
					break
				}
				startSocketID = rsp.SocketRef[len(rsp.SocketRef)-1].SocketId + 1
			}
			writer.WriteString(fmt.Sprintf("Retrieving socket data for %d connections...\n", len(socketRefs)))
			start := time.Now()

			var eg errgroup.Group
			eg.SetLimit(*parallel)

			var mu sync.Mutex
			var sockets []*socketData
			for _, ss := range socketRefs {
				id := ss.SocketId
				eg.Go(func() error {
					sd, err := c.GetSocket(ctx, &czpb.GetSocketRequest{SocketId: id})
					if err != nil {
						log.Warningf("could not fetch server data: %s", err)
						return nil
					}
					tcpAddr := sd.GetSocket().GetRemote().GetTcpipAddress()
					ip := net.IP(tcpAddr.GetIpAddress())
					data := sd.GetSocket().GetData()
					mu.Lock()
					sockets = append(sockets, &socketData{peer: fmt.Sprintf("%15s:%05d", ip, tcpAddr.GetPort()), data: data})
					mu.Unlock()
					return nil
				})
			}

			if err := eg.Wait(); err != nil {
				log.Fatalf("Error getting sockets: %s", err)
			}

			mu.Lock()
			defer mu.Unlock()

			writer.WriteString(fmt.Sprintf("Retrieved socket information in %s\n", time.Since(start)))
			slices.SortFunc(sockets, func(a, b *socketData) int {
				return strings.Compare(a.peer, b.peer)
			})
			log.Infof("Found %d server connections", len(sockets))
			for _, sd := range sockets {
				data := sd.data
				writer.WriteString(fmt.Sprintf("[%s] started %5d | succeeded %5d | failed %5d | running %5d | remote flow control %9d | local flow control %9d\n",
					sd.peer, data.GetStreamsStarted(), data.GetStreamsSucceeded(), data.GetStreamsFailed(),
					data.GetStreamsStarted()-data.GetStreamsSucceeded()-data.GetStreamsFailed(),
					data.GetRemoteFlowControlWindow().GetValue(), data.GetLocalFlowControlWindow().GetValue()))
			}
		}

		rsp, err := c.GetServers(ctx, &czpb.GetServersRequest{})
		if err != nil {
			log.Fatalf("Error getting servers: %s", err)
		}
		for _, s := range rsp.Server {
			log.Infof("Server: %s", s)
			//for _, ls := range s.ListenSocket {
			//	log.Infof("ListenSocket: %s", ls)
			//	sd, err := c.GetSocket(ctx, &czpb.GetSocketRequest{SocketId: ls.SocketId})
			//	if err != nil {
			//		log.Fatalf("Error getting socket: %s", err)
			//	}
			//	log.Infof("Socket data: %s", sd)
			//}
		}
		if err := t.Close(); err != nil {
			log.Fatalf("%s", err)
		}
	}
}

type Target struct {
	Pod        string
	RemotePort int
	LocalPort  int
	Cluster    string
	Namespace  string

	cmd  *exec.Cmd
	stop chan struct{}
	done chan struct{}
}

type targetGroup []*Target

func getTargets() (targetGroup, error) {
	cluster := "gke_flame-build_us-west1_dev-nv8eh"
	if *env == "prod" {
		cluster = "gke_flame-build_us-west1_prod-hs6in"
	}
	ns := "buildbuddy-" + *env
	if strings.HasPrefix(*target, "executor-") {
		ns = "executor-" + *env
	}

	var pods []string
	if *app != "all" {
		if !strings.HasPrefix(*app, "buildbuddy-app-") {
			log.Fatalf("Expected the target app to be in the form 'buildbuddy-app-X'")
		}
		pods = append(pods, *app)
	} else {
		n := 8
		if *env == "prod" {
			n = 24
		}
		for i := 0; i < n; i++ {
			pods = append(pods, fmt.Sprintf("buildbuddy-app-%d", i))
		}
	}

	var out []*Target
	// TODO: To run in parallel, use different local ports for each target
	//localPort := 10_090
	for _, p := range pods {
		out = append(out, &Target{
			Cluster:    cluster,
			Namespace:  ns,
			Pod:        p,
			RemotePort: 1987,
			LocalPort:  1987,

			stop: make(chan struct{}),
			done: make(chan struct{}),
		})
		//localPort++
	}
	return out, nil
}

func (tg targetGroup) Do(ctx context.Context, f func(egCtx context.Context, t *Target) error) error {
	eg, egCtx := errgroup.WithContext(ctx)
	for _, t := range tg {
		t := t
		eg.Go(func() error {
			return f(egCtx, t)
		})
	}
	return eg.Wait()
}

func (t *Target) StartPortForwarding(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			if t.cmd != nil {
				_ = t.cmd.Process.Kill()
			}
			close(t.done)
			return
		}
		go func() {
			<-t.stop
			_ = t.cmd.Process.Kill()
			_ = t.cmd.Wait()
			close(t.done)
		}()
	}()

	cmd := exec.Command(
		"kubectl",
		"port-forward",
		"--cluster="+t.Cluster,
		"--namespace="+t.Namespace,
		t.Pod,
		fmt.Sprintf("%d:%d", t.LocalPort, t.RemotePort),
	)
	log.Printf("Running: %s", cmd)
	if err := cmd.Start(); err != nil {
		return err
	}
	t.cmd = cmd
	// Wait for port forwarding to start
	return t.waitForReady(ctx)
}

func (t *Target) waitForReady(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		conn, err := net.DialTimeout("tcp", t.Addr(), 10*time.Millisecond)
		if err == nil {
			conn.Close()
			log.Printf("%s (%s) %s ready", t.Pod, *env, t.Addr())
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return ctx.Err()
}

func (t *Target) Addr() string {
	return fmt.Sprintf("127.0.0.1:%d", t.LocalPort)
}

func (t *Target) Close() error {
	close(t.stop)
	<-t.done
	log.Printf("Port forwarding on %s stopped", t.Addr())
	return nil
}
