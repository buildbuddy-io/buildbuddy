// vmexec_client connects over the specified VSock and sends commands to the listening vmexec server.
package main

import (
	"context"
	"flag"
	"net"
	"os"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/grpc"

	vmxpb "github.com/buildbuddy-io/buildbuddy/proto/vmexec"
)

var (
	sock             = flag.String("sock", "", "Vsock host socket path")
	cmd              = flag.String("cmd", "pwd", "Command to run")
	path             = flag.String("path", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "The path to use when executing cmd")
	workingDirectory = flag.String("working_directory", "/", "Working directory to run command from")
)

func bufDialer(ctx context.Context, _ string) (net.Conn, error) {
	return vsock.DialHostToGuest(ctx, *sock, vsock.DefaultPort)
}

func main() {
	ctx := context.Background()
	flag.Parse()

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bufDialer),
		grpc.WithInsecure(),
	}
	conn, err := grpc.DialContext(ctx, "vsock", dialOptions...)
	if err != nil {
		log.Fatalf("Error dialing: %s", err)
	}
	defer conn.Close()
	log.Printf("Connected to client")

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
