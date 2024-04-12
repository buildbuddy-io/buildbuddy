// vmexec connects over the specified VSock and sends commands to the listening vmexec server.
//
// To run it on a live executor pod:
//
// $ bazel build enterprise/tools/vmexec
// $ kubectl cp ./bazel-bin/tools/vmexec/vmexec_/vmexec executor-prod/executor-abc-123:/usr/local/bin/vmexec
// $ kubectl exec -n executor-prod executor-abc-123 -- bash
// # vmexec -sock=/buildbuddy/remotebuilds/firecracker/{vmid}/root/run/v.sock sh -c 'echo hello'
//
// To see the VMIDs of all currently running VMs, run:
// $ ps aux | grep /firecracker
//
// The "--id" arg in the firecracker process command line is the VM ID.
package main

import (
	"context"
	"flag"
	"os"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	vmxpb "github.com/buildbuddy-io/buildbuddy/proto/vmexec"
)

var (
	sock             = flag.String("sock", "", "Vsock host socket path")
	path             = flag.String("path", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "The path to use when executing cmd")
	workingDirectory = flag.String("working_directory", "/", "Working directory to run command from")
)

func main() {
	ctx := context.Background()
	flag.Parse()

	if len(flag.Args()) == 0 {
		log.Fatalf("Missing command. Example: vmexec -sock=/path/to/v.sock sh -c 'echo hello'")
	}

	conn, err := vsock.SimpleGRPCDial(ctx, *sock, vsock.VMExecPort)
	if err != nil {
		log.Fatalf("Error connecting to client: %s", err)
	}
	defer conn.Close()

	execClient := vmxpb.NewExecClient(conn)
	rsp, err := execClient.Exec(ctx, &vmxpb.ExecRequest{
		Arguments:        flag.Args(),
		WorkingDirectory: *workingDirectory,
		EnvironmentVariables: []*vmxpb.ExecRequest_EnvironmentVariable{
			{
				Name:  "PATH",
				Value: *path,
			},
		},
	})
	if err != nil {
		log.Fatalf("Error calling exec: %s", err)
	}
	os.Stderr.Write(rsp.Stderr)
	os.Stdout.Write(rsp.Stdout)
}
