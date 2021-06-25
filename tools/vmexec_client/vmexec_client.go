// vmexec_client connects over the specified VSock and sends commands to the listening vmexec server.
package main

import (
	"context"
	"flag"
	"net"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/grpc"

	vmxpb "github.com/buildbuddy-io/buildbuddy/proto/vmexec"
)

var (
	sock             = flag.String("sock", "", "Vsock host socket path")
	cmd              = flag.String("cmd", "pwd", "Command to run")
	workingDirectory = flag.String("working_directory", "/", "Working directory to run command from")
)

func bufDialer(ctx context.Context, _ string) (net.Conn, error) {
	return vsock.HostDial(ctx, *sock, vsock.DefaultPort)
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

	execClient := vmxpb.NewExecClient(conn)
	rsp, err := execClient.Exec(ctx, &vmxpb.ExecRequest{
		Arguments:        strings.Split(*cmd, " "),
		WorkingDirectory: *workingDirectory,
	})
	if err != nil {
		log.Fatalf("Error calling exec: %s", err)
	}
	log.Printf("Exec response: %+v", rsp)
}
