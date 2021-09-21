// vmexec_client connects over the specified VSock and sends commands to the listening vmexec server.
package main

import (
	"context"
	"flag"
	"os"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	vmxpb "github.com/buildbuddy-io/buildbuddy/proto/vmexec"
)

var (
	sock             = flag.String("sock", "", "Vsock host socket path")
	cmd              = flag.String("cmd", "pwd", "Command to run")
	path             = flag.String("path", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "The path to use when executing cmd")
	workingDirectory = flag.String("working_directory", "/", "Working directory to run command from")
)

func main() {
	ctx := context.Background()
	flag.Parse()

	conn, err := vsock.SimpleGRPCDial(ctx, *sock, vsock.VMExecPort)
	if err != nil {
		log.Fatalf("Error connecting to client: %s", err)
	}
	defer conn.Close()

	execClient := vmxpb.NewExecClient(conn)
	rsp, err := execClient.Exec(ctx, &vmxpb.ExecRequest{
		Arguments:        strings.Split(*cmd, " "),
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
