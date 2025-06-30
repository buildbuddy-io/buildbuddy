package tools

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/experimental/modelcontextprotocol/go-sdk/mcp"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"github.com/buildbuddy-io/buildbuddy/server/util/shlex"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/psanford/memfs"
	"google.golang.org/protobuf/types/known/durationpb"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

const digestFunction = repb.DigestFunction_BLAKE3

type ToolHandler struct {
	env environment.Env
}

func NewHandler(env environment.Env) (*ToolHandler, error) {
	return &ToolHandler{
		env: env,
	}, nil
}

/*
   server.AddTools(mcp.NewServerTool("greet", "say hi", SayHi, mcp.Input(
           mcp.Property("name", mcp.Description("the name to say hi to")),
   )))
*/

func (t *ToolHandler) Register(server *mcp.Server) error {
	server.AddTools(
		mcp.NewServerTool("echo_string", "echo back any string with emojis around it", t.Echo),
		mcp.NewServerTool("get_invocation", "list all invocations or get specific invocations by ID", t.GetInvocation),
		mcp.NewServerTool("run_command", "Execute a command. Use this when asked to run any commands.", t.Run, mcp.Input(
			mcp.Property("cmd", mcp.Description("the command to run")))),
		mcp.NewServerTool("file_write", "Create a new file with the provided contents in the working direcotry, overwriting the file if it already exists", t.FileWrite, mcp.Input(
			mcp.Property("path", mcp.Description("The path of the file you want to write")),
			mcp.Property("content", mcp.Description("Full text content of the file you want to write.")),
		)),
	)

	return nil
}

type EchoParams struct {
	Value string `json:"value"`
}

func (t *ToolHandler) Echo(ctx context.Context, cc *mcp.ServerSession, params *mcp.CallToolParamsFor[EchoParams]) (*mcp.CallToolResultFor[any], error) {
	return &mcp.CallToolResultFor[any]{
		Content: []*mcp.Content{
			mcp.NewTextContent(fmt.Sprintf("🎉🎉🎉 %s 🎉🎉🎉", params.Arguments.Value)),
		},
	}, nil
}

type RunParams struct {
	Cmd string `json:"cmd"`
}

func (t *ToolHandler) Run(ctx context.Context, cc *mcp.ServerSession, params *mcp.CallToolParamsFor[RunParams]) (*mcp.CallToolResultFor[any], error) {
	log.Printf("Run() params: %+v", params)
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
	instanceName := "123456" // TODO(tylerw): set to session ID.

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
				{Name: "hosted-bazel-affinity-key", Value: instanceName}, // use sessionID as affinity key.
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
		//InputRootDigest: inputRootDigest,
		DoNotCache: true,
		Timeout:    durationpb.New(10 * time.Second),
	}
	rn, err := rexec.Prepare(ctx, t.env, instanceName, digestFunction, action, cmd, "")
	if err != nil {
		log.Errorf("Prepare error: %s", err)
		return nil, status.WrapError(err, "prepare")
	}
	log.Printf("prepared rn: %s", rn)
	stream, err := rexec.Start(ctx, t.env, rn)
	if err != nil {
		log.Errorf("Start error: %s", err)
		return nil, status.WrapError(err, "start")
	}
	rsp, err := rexec.Wait(stream)
	if err != nil {
		log.Errorf("Wait error: %s", err)
		return nil, status.WrapError(err, "wait")
	}
	commandResult, err := rexec.GetResult(ctx, t.env, instanceName, digestFunction, rsp.ExecuteResponse.GetResult())
	if err != nil {
		log.Errorf("GetResult error: %s", err)
		return nil, status.WrapError(err, "get result")
	}
	log.Printf("commandResult: %+v", commandResult)
	return &mcp.CallToolResultFor[any]{
		Content: []*mcp.Content{
			mcp.NewTextContent(string(commandResult.Stdout)),
		},
	}, nil
}

type GetInvocationParams struct {
	InvocationID *string `json:"invocation_id"`
}

func (t *ToolHandler) GetInvocation(ctx context.Context, cc *mcp.ServerSession, params *mcp.CallToolParamsFor[GetInvocationParams]) (*mcp.CallToolResultFor[any], error) {
	if t.env.GetApiClient() == nil {
		return nil, status.InternalError("no registered api client")
	}
	req := &apipb.GetInvocationRequest{}
	if params.Arguments.InvocationID != nil {
		req.Selector = &apipb.InvocationSelector{
			InvocationId: *params.Arguments.InvocationID,
		}
	}
	_, err := t.env.GetApiClient().GetInvocation(ctx, req)
	if err != nil {
		return nil, err // TODO(tylerw): handle
	}
	return &mcp.CallToolResultFor[any]{
		Content: []*mcp.Content{
			mcp.NewResourceContent(&mcp.ResourceContents{
				URI: "https://app.buildbuddy.io/invocation/" + *params.Arguments.InvocationID,
			}),
		},
	}, nil
}

type FileWriteParams struct {
	Path string `json:"path"`
	Content string `json:"content"`
}

func (t *ToolHandler) FileWrite(ctx context.Context, cc *mcp.ServerSession, params *mcp.CallToolParamsFor[FileWriteParams]) (*mcp.CallToolResultFor[any], error) {
	if params.Arguments.Path == "" {
		return nil, status.InvalidArgumentError("A path is required")
	}
	sessionID, ok := ctx.Value("mcp-session-id").(string)
	if !ok {
		return nil, status.InvalidArgumentError("No session ID found")
	}
	log.Printf("sessionID: %q", sessionID)
	fp := strings.TrimPrefix(params.Arguments.Path, "file://")
	fs := NewSerializableFS()
	if err := fs.MkdirAll(filepath.Dir(fp), 0o777); err != nil {
		return nil, err
	}
	if err := fs.WriteFile(fp, []byte(params.Arguments.Content), 0o777); err != nil {
		return nil, err
	}
	return &mcp.CallToolResultFor[any]{
		Content: []*mcp.Content{
			mcp.NewTextContent(fmt.Sprintf("Wrote file: %s", params.Arguments.Path)),
		},
	}, nil
}

type SerializableMemFS struct {
	*memfs.FS
}

func NewSerializableFS() *SerializableMemFS {
	return &SerializableMemFS{
		FS: memfs.New(),
	}
}

func (fs *SerializableMemFS) Marshal() ([]byte, error) {
	return nil, status.UnimplementedError("no")
}

func Unmarshal(buf []byte) (*SerializableMemFS, error) {
	return nil, status.UnimplementedError("no")
}
