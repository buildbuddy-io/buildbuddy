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
	socket = flag.String("socket", "", "Listen path")
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
