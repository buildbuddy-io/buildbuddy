package printlog

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
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
	cmd, idx := arg.GetCommandAndIndex(args)
	if cmd != flags.Name() {
		return -1, nil
	}
	if err := arg.ParseFlagSet(flags, args[idx+1:]); err != nil {
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
	pr := NewDelimitedProtoReader(grpcLog)
	for {
		err := pr.Unmarshal(m)
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

type DelimitedProtoReader struct {
	buf bytes.Buffer
	r   *bufio.Reader
}

func NewDelimitedProtoReader(r io.Reader) *DelimitedProtoReader {
	return &DelimitedProtoReader{r: bufio.NewReader(r)}
}

func (p *DelimitedProtoReader) Unmarshal(m proto.Message) error {
	size, err := binary.ReadUvarint(p.r)
	if err != nil {
		return err
	}
	p.buf.Reset()
	if _, err := io.CopyN(&p.buf, p.r, int64(size)); err != nil {
		return err
	}
	return proto.Unmarshal(p.buf.Bytes(), m)
}
