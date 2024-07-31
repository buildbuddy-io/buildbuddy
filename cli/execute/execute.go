package execute

import (
	"context"
	"os"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/mdutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var flags = flag.NewFlagSet("execute", flag.ContinueOnError)

// Bazel-equivalent flags.
var (
	target         = flags.String("remote_executor", "grpcs://remote.buildbuddy.io", "Remote execution service target.")
	instanceName   = flags.String("remote_instance_name", "", "Value to pass as an instance_name in the remote execution API.")
	digestFunction = flags.String("digest_function", "sha256", "Digest function used for content-addressable storage. Can be `\"sha256\" or \"blake3\"`.")
	invocationID   = flags.String("invocation_id", "", "If set, set this value as the tool_invocation_id in RequestMetadata.")
	timeout        = flags.Duration("remote_timeout", 1*time.Hour, "Timeout used for the action.")
	remoteHeaders  = flag.New(flags, "remote_header", []string{}, "Header to be applied to all outgoing gRPC requests, as a `NAME=VALUE` pair. Can be specified more than once.")
	actionEnv      = flag.New(flags, "action_env", []string{}, "Action environment variable, as a `NAME=VALUE` pair. Can be specified more than once.")
)

// Flags specific to `bb execute`.
var (
	inputRoot = flags.String("input_root", "", "Input root directory. By default, the action will have no inputs.")
	// Note: bazel has remote_default_exec_properties but it has somewhat
	// confusing semantics, so we call this "exec_properties" to avoid
	// confusion.
	execProperties = flag.New(flags, "exec_properties", []string{}, "Platform exec property, as a `NAME=VALUE` pair. Can be specified more than once.")
)

const (
	usage = `
usage: bb execute [ options ... ] -- <executable> [ args ... ]

Runs a remote execution request against a remote execution service backend
using a given command as input.

Args that modify execution can be placed before '--', and the command executable
and arguments should come afterwards.

Example of running a simple bash command:
  $ bb execute -- bash -c 'echo "Hello world!"'

Example of running a bash command with runner recycling:
  $ bb execute --exec_properties=recycle-runner=true -- bash -c 'echo "Runner uptime:" $(uptime)'
`
)

func HandleExecute(args []string) (int, error) {
	args, cmdArgs := arg.SplitExecutableArgs(args)
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			log.Print("\nAll options:")
			flags.SetOutput(os.Stderr)
			flags.PrintDefaults()
			return 1, nil
		}
		return -1, err
	}
	if len(flag.Args()) > 0 {
		log.Print("error: command executable and arguments must appear after arg separator '--'")
		log.Print(usage)
		return 1, nil
	}
	if len(cmdArgs) == 0 {
		log.Print("error: must provide arg separator '--' followed by command")
		log.Print(usage)
		return 1, nil
	}
	if err := execute(cmdArgs); err != nil {
		return -1, err
	}
	return 0, nil
}

func execute(cmdArgs []string) error {
	ctx := context.Background()
	md, err := mdutil.Parse(*remoteHeaders...)
	if err != nil {
		return err
	}
	ctx = metadata.NewOutgoingContext(ctx, md)

	iid := *invocationID
	if iid == "" {
		iid = uuid.New()
	}
	rmd := &repb.RequestMetadata{ToolInvocationId: iid}
	ctx, err = bazel_request.WithRequestMetadata(ctx, rmd)
	if err != nil {
		return err
	}

	conn, err := grpc_client.DialSimple(*target)
	if err != nil {
		return err
	}
	env := real_environment.NewBatchEnv()
	env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	env.SetRemoteExecutionClient(repb.NewExecutionClient(conn))
	env.SetCapabilitiesClient(repb.NewCapabilitiesClient(conn))

	environ, err := rexec.MakeEnv(*actionEnv...)
	if err != nil {
		return err
	}
	platform, err := rexec.MakePlatform(*execProperties...)
	if err != nil {
		return err
	}
	cmd := &repb.Command{
		Arguments:            cmdArgs,
		EnvironmentVariables: environ,
		Platform:             platform,
	}
	action := &repb.Action{}
	if *timeout > 0 {
		action.Timeout = durationpb.New(*timeout)
	}
	// TODO: use capabilities client and respect remote digest function &
	// compressor.
	df, err := digest.ParseFunction(*digestFunction)
	if err != nil {
		return err
	}
	start := time.Now()
	stageStart := start
	log.Debugf("Preparing action for %s", cmd)
	arn, err := rexec.Prepare(ctx, env, *instanceName, df, action, cmd, *inputRoot)
	if err != nil {
		return err
	}
	log.Debugf("Uploaded inputs in %s", time.Since(stageStart))
	actionStr, err := digest.ResourceNameFromProto(arn).DownloadString()
	if err != nil {
		log.Debugf("Failed to compute action resource name: %s", err)
	} else {
		log.Debugf("Action resource name: %s", actionStr)
	}
	stageStart = time.Now()
	log.Debug("Starting /Execute request")
	stream, err := rexec.Start(ctx, env, arn)
	if err != nil {
		return err
	}
	log.Debugf("Waiting for execution to complete")
	var rsp *rexec.Response
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		if msg.Err != nil {
			// We failed to execute.
			return msg.Err
		}
		// Log execution state
		progress := &repb.ExecutionProgress{}
		ok, _ := rexec.AuxiliaryMetadata(msg.ExecuteOperationMetadata.GetPartialExecutionMetadata(), progress)
		if ok && progress.GetExecutionState() != 0 {
			log.Debugf(
				"Remote: %s @ %s",
				repb.ExecutionProgress_ExecutionState_name[int32(progress.GetExecutionState())],
				progress.GetTimestamp().AsTime(),
			)
		} else {
			log.Debugf("Remote: %s", repb.ExecutionStage_Value_name[int32(msg.ExecuteOperationMetadata.GetStage())])
		}
		if msg.Done {
			rsp = msg
			break
		}
	}
	log.Debugf("Execution completed in %s", time.Since(stageStart))
	stageStart = time.Now()
	log.Debugf("Downloading result")
	res, err := rexec.GetResult(ctx, env, *instanceName, df, rsp.ExecuteResponse.GetResult())
	if err != nil {
		return status.WrapError(err, "execution failed")
	}
	log.Debugf("Downloaded results in %s", time.Since(stageStart))
	log.Debugf("End-to-end execution time: %s", time.Since(start))

	executionMetadata := rsp.ExecuteResponse.GetResult().GetExecutionMetadata()
	if b, err := protojson.Marshal(executionMetadata); err == nil {
		log.Debugf("Execution metadata: %s", string(b))
	}

	os.Stdout.Write(res.Stdout)
	os.Stderr.Write(res.Stderr)

	return nil
}
