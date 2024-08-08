package persistentworker

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/workspace"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/lockingbuffer"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	wkpb "github.com/buildbuddy-io/buildbuddy/proto/worker"
)

const (
	// How long to spend waiting for a persistent worker process to terminate
	// after we send the shutdown signal before giving up.
	persistentWorkerShutdownTimeout = 10 * time.Second

	// Protocol value identifying the JSON persistent worker protocol.
	jsonProtocol = "json"

	// Protocol value identifying the protobuf persistent worker protocol.
	protobufProtocol = "proto"
)

var (
	flagFilePattern           = regexp.MustCompile(`^(?:@|--?flagfile=)(.+)`)
	externalRepositoryPattern = regexp.MustCompile(`^@.*//.*`)
)

// Worker represents a persistent worker process that receives commands over
// stdin and sends responses on stdout.
type Worker struct {
	workspace *workspace.Workspace
	container container.CommandContainer
	protocol  string // "json" or "proto"

	stdinWriter *io.PipeWriter
	stderr      lockingbuffer.LockingBuffer

	stdoutReader *bufio.Reader
	jsonDecoder  *json.Decoder

	stop func() error
}

// Start spawns a persistent worker inside the given container using
// a long-running Exec() command.
// The provided context should be a long-lived context that lives longer
// than just a single task.
func Start(ctx context.Context, workspace *workspace.Workspace, container container.CommandContainer, protocol string, command *repb.Command) *Worker {
	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()

	w := &Worker{
		container: container,
		workspace: workspace,
		protocol:  protocol,

		stdinWriter:  stdinWriter,
		stdoutReader: bufio.NewReader(stdoutReader),
	}
	if protocol == jsonProtocol {
		w.jsonDecoder = json.NewDecoder(stdoutReader)
	}

	ctx, cancel := context.WithCancel(ctx)
	workerTerminated := make(chan struct{})
	w.stop = func() error {
		// Canceling the worker context and closing stdin should terminate the
		// worker exec process.
		cancel()
		_ = stdinWriter.Close()
		// Wait for the worker to terminate. This is needed since canceling the
		// context doesn't block until the worker is killed. This helps ensure that
		// the worker is killed if we are shutting down. The shutdown case is also
		// why we use ExtendContextForFinalization here.
		ctx, cancel := background.ExtendContextForFinalization(ctx, persistentWorkerShutdownTimeout)
		defer cancel()
		select {
		case <-workerTerminated:
			return nil
		case <-ctx.Done():
			return status.DeadlineExceededError("Timed out waiting for persistent worker to shut down.")
		}
	}

	args := parseArgs(command.GetArguments())
	command = command.CloneVT()
	command.Arguments = append(args.WorkerArgs, "--persistent_worker")

	go func() {
		defer close(workerTerminated)
		defer stdinReader.Close()
		defer stdoutWriter.Close()

		stdio := &interfaces.Stdio{
			Stdin:  stdinReader,
			Stdout: stdoutWriter,
			Stderr: &w.stderr,
		}
		res := w.container.Exec(ctx, command, stdio)
		log.Debugf("Persistent worker exited with response: %+v, flagFiles: %+v, workerArgs: %+v", res, args.FlagFiles, args.WorkerArgs)
	}()

	return w
}

func (w *Worker) Exec(ctx context.Context, command *repb.Command) *interfaces.CommandResult {
	// Clear any stderr that might be associated with a previous request.
	w.stderr.Reset()

	args := parseArgs(command.GetArguments())
	expandedArguments, err := w.expandFlagFiles(args.FlagFiles)
	if err != nil {
		return commandutil.ErrorResult(status.WrapError(err, "expand flag files"))
	}

	// Collect all of the input digests.
	inputs := make([]*wkpb.Input, 0, len(w.workspace.Inputs))
	for path, digest := range w.workspace.Inputs {
		digestBytes, err := proto.Marshal(digest)
		if err != nil {
			return commandutil.ErrorResult(status.WrapError(err, "marshal input digest"))
		}
		inputs = append(inputs, &wkpb.Input{
			Digest: digestBytes,
			Path:   path,
		})
	}

	// Write the encoded request to stdin.
	req := &wkpb.WorkRequest{
		Inputs:    inputs,
		Arguments: expandedArguments,
	}
	if err := w.marshalWorkRequest(req); err != nil {
		return commandutil.ErrorResult(status.UnavailableErrorf(
			"failed to send persistent work request: %s\npersistent worker stderr:\n%s",
			err, w.stderrDebugString()))
	}

	// Decode the response from stdout.
	rsp := &wkpb.WorkResponse{}
	if err := w.unmarshalWorkResponse(rsp); err != nil {
		return commandutil.ErrorResult(status.UnavailableErrorf(
			"failed to read persistent work response: %s\npersistent worker stderr:\n%s",
			err, w.stderrDebugString()))
	}
	return &interfaces.CommandResult{
		Stderr:   []byte(rsp.Output),
		ExitCode: int(rsp.ExitCode),
	}
}

// Stop kills the worker process and waits for it to exit.
func (w *Worker) Stop() error {
	return w.stop()
}

func (w *Worker) stderrDebugString() string {
	s := w.stderr.String()
	if s == "" {
		return "<empty>"
	}
	return s
}

func (r *Worker) marshalWorkRequest(requestProto *wkpb.WorkRequest) error {
	if r.protocol == jsonProtocol {
		marshaler := &protojson.MarshalOptions{EmitUnpopulated: true}
		out, err := marshaler.Marshal(requestProto)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(r.stdinWriter, "%s\n", string(out))
		return err
	}
	// TODO: return this error in Start()
	if r.protocol != "" && r.protocol != protobufProtocol {
		return status.FailedPreconditionErrorf("unsupported persistent worker protocol %q", r.protocol)
	}
	// Write the proto length (in varint encoding), then the proto itself
	buf := protowire.AppendVarint(nil, uint64(proto.Size(requestProto)))
	var err error
	buf, err = proto.MarshalOptions{}.MarshalAppend(buf, requestProto)
	if err != nil {
		return err
	}
	_, err = r.stdinWriter.Write(buf)
	return err
}

func (w *Worker) unmarshalWorkResponse(responseProto *wkpb.WorkResponse) error {
	if w.protocol == jsonProtocol {
		raw := json.RawMessage{}
		if err := w.jsonDecoder.Decode(&raw); err != nil {
			return err
		}
		return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(raw, responseProto)
	}
	// TODO: return this error from Start()
	if w.protocol != "" && w.protocol != protobufProtocol {
		return status.FailedPreconditionErrorf("unsupported persistent worker protocol %q", w.protocol)
	}
	// Read the response size from stdout as a unsigned varint.
	size, err := binary.ReadUvarint(w.stdoutReader)
	if err != nil {
		return err
	}
	data := make([]byte, size)
	// Read the response proto from stdout.
	if _, err := io.ReadFull(w.stdoutReader, data); err != nil {
		return err
	}
	if err := proto.Unmarshal(data, responseProto); err != nil {
		return err
	}
	return nil
}

// Recursively expands arguments by replacing @filename args with the contents
// of the referenced files. The @ itself can be escaped with @@. This
// deliberately does not expand --flagfile= style arguments, because we want to
// get rid of the expansion entirely at some point in time.
//
// Based on:
// https://github.com/bazelbuild/bazel/blob/e9e6978809b0214e336fee05047d5befe4f4e0c3/src/main/java/com/google/devtools/build/lib/worker/WorkerSpawnRunner.java#L324
func (w *Worker) expandFlagFiles(args []string) ([]string, error) {
	expandedArgs := make([]string, 0)
	for _, arg := range args {
		if strings.HasPrefix(arg, "@") && !strings.HasPrefix(arg, "@@") && !externalRepositoryPattern.MatchString(arg) {
			file, err := os.Open(filepath.Join(w.workspace.Path(), arg[1:]))
			if err != nil {
				return nil, err
			}
			defer file.Close()
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				args, err := w.expandFlagFiles([]string{scanner.Text()})
				if err != nil {
					return nil, err
				}
				expandedArgs = append(expandedArgs, args...)
			}
			if err := scanner.Err(); err != nil {
				return nil, err
			}
		} else {
			expandedArgs = append(expandedArgs, arg)
		}
	}

	return expandedArgs, nil
}

type parsedArgs struct {
	WorkerArgs []string
	FlagFiles  []string
}

func parseArgs(args []string) parsedArgs {
	parsed := parsedArgs{}
	for _, arg := range args {
		if flagFilePattern.MatchString(arg) {
			parsed.FlagFiles = append(parsed.FlagFiles, arg)
		} else {
			parsed.WorkerArgs = append(parsed.WorkerArgs, arg)
		}
	}
	return parsed
}

// Key returns the persistent worker key for a task.
// Tasks are only routed to workers that previously ran a task with an identical
// key.
// It returns ("", false) if the task does not support persistent workers.
func Key(props *platform.Properties, commandArgs []string) (key string, ok bool) {
	if props.PersistentWorkerKey != "" {
		return props.PersistentWorkerKey, true
	}
	if !props.PersistentWorker {
		return "", false
	}
	a := parseArgs(commandArgs)
	if len(a.FlagFiles) == 0 {
		return "", false
	}
	return strings.Join(a.WorkerArgs, " "), true
}
