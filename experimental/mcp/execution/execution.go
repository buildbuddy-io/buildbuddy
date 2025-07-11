package execution

import (
	"context"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"github.com/buildbuddy-io/buildbuddy/server/util/shlex"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/protobuf/types/known/durationpb"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const (
	digestFunction = repb.DigestFunction_BLAKE3
)

type Handler struct {
	env environment.Env
}

func NewHandler(env environment.Env) (*Handler, error) {
	return &Handler{
		env: env,
	}, nil
}

func (t *Handler) Register(server *mcp.Server) error {
	mcp.AddTool(server, &mcp.Tool{Name: "execute_command", Description: "Execute a command on buildbuddy"}, t.Run)
	return nil
}

type RunParams struct {
	Cmd string `json:"cmd" jsonschema:"A unix command to run on a remote buildbuddy machine"`
}

func (t *Handler) Run(ctx context.Context, cc *mcp.ServerSession, params *mcp.CallToolParamsFor[RunParams]) (*mcp.CallToolResultFor[any], error) {
	if params.Arguments.Cmd == "" {
		return nil, status.InvalidArgumentError("cmd must be set")
	}
	executionClient := t.env.GetRemoteExecutionClient()
	if executionClient == nil {
		return nil, status.InternalError("no registered execution client")
	}
	bytestreamClient := t.env.GetByteStreamClient()
	if bytestreamClient == nil {
		return nil, status.InternalError("no registered bytestream client")
	}
	instanceName := "/mcp/" + cc.ID()
	args, err := shlex.Split(params.Arguments.Cmd)
	if err != nil {
		return nil, status.InternalErrorf("invalid command: %s", err)
	}
	cmd := &repb.Command{
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "MCP", Value: "true"},
			{Name: "PATH", Value: "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
		},
		Arguments: args,
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "Pool", Value: "workflows"},
				{Name: "hosted-bazel-affinity-key", Value: cc.ID()}, // use sessionID as affinity key.
				{Name: "container-image", Value: "docker://gcr.io/flame-public/rbe-ubuntu24-04@sha256:f7db0d4791247f032fdb4451b7c3ba90e567923a341cc6dc43abfc283436791a"},
				{Name: "recycle-runner", Value: "true"},
				{Name: "preserve-workspace", Value: "true"},
				{Name: "workload-isolation-type", Value: "firecracker"},
				{Name: "retry", Value: "true"},
				{Name: "EstimatedComputeUnits", Value: "1"},
				{Name: "EstimatedFreeDiskBytes", Value: "2000000000"}, // 2GB
			},
		},
	}
	rexec.NormalizeCommand(cmd)
	action := &repb.Action{
		DoNotCache: true,
		Timeout:    durationpb.New(10 * time.Second),
	}
	rn, err := rexec.Prepare(ctx, t.env, instanceName, digestFunction, action, cmd, "")
	if err != nil {
		return nil, status.WrapError(err, "prepare")
	}
	log.Printf("Started remote execution of: %s", params.Arguments.Cmd)
	stream, err := rexec.Start(ctx, t.env, rn)
	if err != nil {
		return nil, status.WrapError(err, "start")
	}
	rsp, err := rexec.Wait(stream)
	if err != nil {
		return nil, status.WrapError(err, "wait")
	}
	commandResult, err := rexec.GetResult(ctx, t.env, instanceName, digestFunction, rsp.ExecuteResponse.GetResult())
	if err != nil {
		log.Errorf("GetResult error: %s", err)
		return nil, status.WrapError(err, "get result")
	}
	log.Printf("Result: %+v (%s)", commandResult, commandResult.Stdout)
	return &mcp.CallToolResultFor[any]{
		Content: []mcp.Content{
			&mcp.TextContent{Text: string(commandResult.Stdout)},
		},
	}, nil
}
