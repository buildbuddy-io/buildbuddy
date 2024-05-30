// execserver is a little program that can be run inside a container to accept
// commands from the host. Its purpose is to provide an alternative to things
// like 'docker exec' or 'podman exec' which have significant overhead.
//
// It listens on a unix socket, which should be mounted into the container from
// the host. It exposes the Exec service, which can be called using the
// vmexec_client package.

package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/vmexec"
	"google.golang.org/grpc"

	vmxpb "github.com/buildbuddy-io/buildbuddy/proto/vmexec"
)

var (
	socket = flag.String("socket", "", "Unix socket path for serving the exec service")
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	if err := os.RemoveAll(*socket); err != nil {
		return fmt.Errorf("error removing existing socket: %w", err)
	}
	lis, err := net.Listen("unix", *socket)
	if err != nil {
		return err
	}
	srv, err := vmexec.NewServer("")
	if err != nil {
		return err
	}
	s := grpc.NewServer(grpc.MaxRecvMsgSize(50_000_000))
	vmxpb.RegisterExecServer(s, srv)
	return s.Serve(lis)
}
