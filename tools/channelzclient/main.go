package main

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	czpb "github.com/buildbuddy-io/buildbuddy/proto/channelz"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"golang.org/x/sync/errgroup"
)

var (
	target     = flag.String("target", "grpc://localhost:1987", "target host:port")
	serverID   = flag.Int64("server_id", 0, "server ID")
	serverName = flag.String("server_name", "", "server name")
	parallel   = flag.Int("p", 50, "paralellism when fetching socket information")
)

type socketData struct {
	peer string
	data *czpb.SocketData
}

func main() {
	flag.Parse()

	conn, err := grpc_client.DialSimple(*target)
	if err != nil {
		log.Fatalf("Error creating grpc client: %s", err)
	}

	ctx := context.Background()
	c := czpb.NewChannelzClient(conn)

	if *serverName != "" {
		log.Infof("Looking up server %q", *serverName)
		rsp, err := c.GetServers(ctx, &czpb.GetServersRequest{})
		if err != nil {
			log.Fatalf("Error getting servers: %s", err)
		}
		for _, s := range rsp.Server {
			for _, ls := range s.ListenSocket {
				if ls.GetName() == *serverName {
					*serverID = s.GetRef().GetServerId()
					break
				}
			}
			if *serverID != 0 {
				break
			}
		}
		if *serverID == 0 {
			log.Fatalf("Could not find server %q", *serverName)
		}
	}
	if *serverID != 0 {
		log.Infof("Getting server sockets for %d", *serverID)
		startSocketID := int64(0)
		var socketRefs []*czpb.SocketRef
		for {
			log.Infof("Getting server sockets starting with %d", startSocketID)
			rsp, err := c.GetServerSockets(ctx, &czpb.GetServerSocketsRequest{ServerId: *serverID, StartSocketId: startSocketID, MaxResults: 1000})
			if err != nil {
				log.Fatalf("Error getting server sockets: %s", err)
			}
			socketRefs = append(socketRefs, rsp.GetSocketRef()...)
			if rsp.GetEnd() {
				break
			}
			startSocketID = rsp.SocketRef[len(rsp.SocketRef)-1].SocketId + 1
		}
		log.Infof("Retrieving socket data for %d connections...", len(socketRefs))
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

		log.Infof("Retrieved socket information in %s", time.Since(start))
		slices.SortFunc(sockets, func(a, b *socketData) int {
			return strings.Compare(a.peer, b.peer)
		})
		log.Infof("Found %d server connections", len(sockets))
		for _, sd := range sockets {
			data := sd.data
			log.Infof("[%s] started %5d succeeded %5d failed %5d running %5d remote flow control %9d local flow control %9d",
				sd.peer, data.GetStreamsStarted(), data.GetStreamsSucceeded(), data.GetStreamsFailed(),
				data.GetStreamsStarted()-data.GetStreamsSucceeded()-data.GetStreamsFailed(),
				data.GetRemoteFlowControlWindow().GetValue(), data.GetLocalFlowControlWindow().GetValue())
		}
		return
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
}
