package printlog

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/printlog/compact"
	"github.com/buildbuddy-io/buildbuddy/cli/printlog/detect"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/encoding/protojson"

	// Need to init this so that we can marshal messages type Any such as OriginMetadata
	_ "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rlpb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution_log"
)

const (
	usage = `
usage: bb print [PATH] [--grpc_log=PATH] [--compact_execution_log=PATH] [--sort=false] [--raw=false] [--max_entry_size_mb=40]

Prints a Bazel log in human-readable form, detecting its format when PATH is provided.

Currently supported log types:
  --grpc_log: Path to a file saved with --experimental_remote_grpc_log.
  --compact_execution_log: Path to a file saved with --experimental_execution_log_compact_file.
  --sort: Apply sorting to log output, only applicable with --compact_execution_log.
  --raw: Don't convert the log entries to Bazel's Spawn, only applicable with --compact_execution_log.
`
)

var (
	flags          = flag.NewFlagSet("print", flag.ContinueOnError)
	Flags          = flags
	grpcLog        = flags.String("grpc_log", "", "gRPC log path.")
	compactExecLog = flags.String("compact_execution_log", "", "compact execution log path.")
	sort           = flags.Bool("sort", false, "apply sorting to log output, only applicable with --compact_execution_log")
	raw            = flags.Bool("raw", false, "don't convert the log entries to Bazel's Spawn, only applicable with --compact_execution_log")
	maxEntrySizeMB = flags.Int64("max_entry_size_mb", 40, "maximum size in MB of proto log entry that can be unmarshalled")
)

func HandlePrint(args []string) (int, error) {
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return -1, err
	}
	if flags.NArg() > 1 {
		return -1, fmt.Errorf("expected at most one log file, got %d", flags.NArg())
	}
	if flags.NArg() == 1 && (*grpcLog != "" || *compactExecLog != "") {
		return -1, fmt.Errorf("cannot pass a log file alongside --grpc_log or --compact_execution_log")
	}
	if *grpcLog != "" {
		if err := printLog(*grpcLog, &rlpb.LogEntry{}); err != nil {
			return -1, err
		}
		return 0, nil
	}
	if *compactExecLog != "" {
		if err := compact.PrintCompactExecLog(*compactExecLog, *raw, *sort); err != nil {
			return -1, err
		}
		return 0, nil
	}
	if flags.NArg() == 1 {
		if err := printDetected(flags.Arg(0)); err != nil {
			return -1, err
		}
		return 0, nil
	}
	log.Print(usage)
	return 1, nil
}

func printDetected(path string) error {
	// Detection reads the file and the printer opens it again, so it has to be
	// re-readable from the start. A pipe or process substitution isn't.
	if st, err := os.Stat(path); err == nil && !st.Mode().IsRegular() {
		return fmt.Errorf("cannot detect the format of %q: not a regular file; pass --grpc_log or --compact_execution_log instead", path)
	}
	format, err := detect.FileFormat(path)
	if err != nil {
		return fmt.Errorf("detect log format for %q: %w", path, err)
	}
	switch format {
	case detect.GRPCLog:
		return printLog(path, &rlpb.LogEntry{})
	case detect.CompactExecutionLog:
		return compact.PrintCompactExecLog(path, *raw, *sort)
	default:
		return fmt.Errorf("unsupported log format %q", format)
	}
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
		err := protodelim.UnmarshalOptions{MaxSize: *maxEntrySizeMB << 20}.UnmarshalFrom(br, m)
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
