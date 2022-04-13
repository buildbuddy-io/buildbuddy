// This is a test binary for integration testing remote build execution.
// When the binary is executed it connects to a controller and blocks until it receives
// further instruction from the test.
package main

import (
	"context"
	"flag"
	"io"
	"os"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/grpc"

	retpb "github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/proto"
	"google.golang.org/protobuf/encoding/prototext"
)

const (
	abnormalTerminationExitCode = 100
)

var (
	controller = flag.String("controller", "", "Address of controller gRPC server")
	name       = flag.String("name", "", "Name used to register with the controller")
)

func main() {
	flag.Parse()

	if *controller == "" {
		log.Print("--controller is required")
		os.Exit(abnormalTerminationExitCode)
	}

	if *name == "" {
		log.Print("--name is required")
		os.Exit(abnormalTerminationExitCode)
	}

	conn, err := grpc.Dial(*controller, grpc.WithInsecure(), grpc.WithTimeout(10*time.Second), grpc.WithBlock())
	if err != nil {
		log.Printf("Could not connect to controller: %v", err)
		os.Exit(abnormalTerminationExitCode)
	}

	ctx := context.Background()
	client := retpb.NewCommandControllerClient(conn)

	req := &retpb.RegisterCommandRequest{
		CommandName: *name,
	}

	stream, err := client.RegisterCommand(ctx)
	if err != nil {
		log.Printf("Could not register with the controller: %v", err)
		os.Exit(abnormalTerminationExitCode)
	}

	if err := stream.Send(req); err != nil {
		log.Warningf("Could not send registration request: %s", err)
		os.Exit(abnormalTerminationExitCode)
	}

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			log.Printf("Controller unexpectedly closed stream.")
			os.Exit(abnormalTerminationExitCode)
		}
		if err != nil {
			log.Printf("Received error from controller: %v", err)
			os.Exit(abnormalTerminationExitCode)
		}

		switch {
		case req.ExitOp != nil:
			os.Exit(int(req.GetExitOp().GetExitCode()))
		default:
			out, _ := prototext.Marshal(req)
			log.Printf("Unexpected op requested by controller: %s", out)
			os.Exit(abnormalTerminationExitCode)
		}
	}
}
