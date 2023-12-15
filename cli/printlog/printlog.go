package printlog

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	rlpb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution_log"
)

const (
	usage = `
usage: bb print --grpc_log=PATH

Prints a human-readable representation of log files output by Bazel.

Currently supported log types:
  --grpc_log: Path to a file saved with --experimental_remote_grpc_log.
`
)

var (
	flags   = flag.NewFlagSet("print", flag.ContinueOnError)
	grpcLog = flags.String("grpc_log", "", "gRPC log path.")
)

func HandlePrint(args []string) (int, error) {
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return -1, err
	}
	if *grpcLog != "" {
		if err := printLog(*grpcLog, &rlpb.LogEntry{}); err != nil {
			return -1, err
		}
		return 0, nil
	}
	log.Print(usage)
	return 1, nil
}

func printLog(path string, m proto.Message) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := copyUnmarshaled(os.Stdout, f, m); err != nil {
		return err
	}
	return nil
}

func copyUnmarshaled(w io.Writer, grpcLog io.Reader, m proto.Message) error {
	br := bufio.NewReader(grpcLog)
	for {
		err := protodelim.UnmarshalFrom(br, m)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to read LogEntry: %s", err)
		}
		b, err := protojson.MarshalOptions{Multiline: true}.Marshal(m)
		if err != nil {
			return fmt.Errorf("failed to marshal remote gRPC log entry: %s", err)
		}
		if _, err := w.Write(b); err != nil {
			return err
		}
		if _, err := w.Write([]byte{'\n'}); err != nil {
			return err
		}
	}
}
