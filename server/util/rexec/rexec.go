// package rexec provides a command interface similar to the "os/exec" package
// but runs commands using a remote execution client.
//
// Basic usage: you can just call Result() to run a command end-to-end,
// including uploading the Action, Command, and input root dir to cache:
//
//	cmd := rexec.Command(env, "echo", "hello")
//	res, err := cmd.Result(ctx)
//	if err != nil {
//		return err
//	}
//	fmt.Println(string(res.Stdout)) // prints "hello"
//
// You can also manually walk the command through its lifecycle:
//
//	// TODO: handle errors!
//	cmd := rexec.Command(env, "echo" "hello")
//	err := cmd.Upload(ctx)
//	err = cmd.Start(ctx)
//	err = cmd.Wait(ctx)
//	res, err := cmd.Result(ctx)
//
// Command properties are configured by setting attributes on *Cmd:
//
//	cmd.Dir = "/path/to/input_root_dir"
//	cmd.Env = []string{"FOO=BAR"}
//	cmd.Platform = []string{"recycle-runner=true"}
package rexec

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/protobuf/types/known/durationpb"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	gstatus "google.golang.org/grpc/status"
)

const (
	defaultDigestFunction = repb.DigestFunction_SHA256
)

type state int

const (
	initial state = iota
	uploading
	starting
	waiting
)

// Cmd provides a handle on a remote command.
// It is not safe for concurrent use.
type Cmd struct {
	// Arguments are the executable and arguments for the command.
	Arguments []string
	// Dir is a local dir to be used as the action's root directory. The current
	// directory contents will be uploaded from there, and outputs will be
	// written there.
	//
	// Unlike exec.Cmd, an empty string here means "do not upload inputs or
	// download outputs", rather than "use the current working directory".
	Dir string
	// Env is a slice of "NAME=VALUE" pairs to be used as environment variables.
	// Unlike exec.Cmd, a nil value here means "use an empty env" instead of
	// "use the current command's environment".
	Env []string

	// InstanceName is the remote instance name.
	InstanceName string
	// Platform is a slice of "NAME=VALUE" pairs to be used as platform
	// properties.
	Platform []string
	// DigestFunction is the digest function to use for CAS transfers (defaults
	// to SHA256).
	DigestFunction repb.DigestFunction_Value
	// Timeout is the timeout to be applied to the remote command. Note that the
	// context timeout used for execution only applies to the RPC. Unless this
	// timeout is also specified, the remote execution may continue beyond the
	// local context deadline.
	Timeout time.Duration

	env           environment.Env
	state         state
	actionDigest  *repb.Digest
	stream        *RetryingStream
	response      *repb.ExecuteResponse
	waitErr       error
	commandResult *interfaces.CommandResult
}

// Command returns a new Cmd from the given executable and arguments.
func Command(env environment.Env, executable string, args ...string) *Cmd {
	return &Cmd{
		env:       env,
		Arguments: append([]string{executable}, args...),
	}
}

// Close closes any streams established to the server.
func (c *Cmd) Close() error {
	if c.stream != nil {
		return c.stream.CloseSend()
	}
	return nil
}

func (c *Cmd) String() string {
	return fmt.Sprintf("%s", c.Arguments)
}

// Upload transfers all of the data to cache that is needed to run the action.
// This includes the Action and Command protos, as well as the input tree.
func (c *Cmd) Upload(ctx context.Context) error {
	if c.state >= uploading {
		return nil
	}
	c.state = uploading

	p, err := AssemblePlatform(c.Platform...)
	if err != nil {
		return err
	}
	envVars, err := AssembleEnvironmentVariables(c.Env...)
	if err != nil {
		return err
	}
	cmd := &repb.Command{
		Arguments:            c.Arguments,
		EnvironmentVariables: envVars,
		Platform:             p,
	}
	var commandDigest, inputRootDigest *repb.Digest
	eg, egctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		ctx := egctx
		d, err := cachetools.UploadProto(ctx, c.env.GetByteStreamClient(), c.InstanceName, c.digestFunction(), cmd)
		if err != nil {
			return err
		}
		commandDigest = d
		return nil
	})
	if c.Dir != "" {
		eg.Go(func() error {
			ctx := egctx
			d, _, err := cachetools.UploadDirectoryToCAS(ctx, c.env, c.InstanceName, c.digestFunction(), c.Dir)
			if err != nil {
				return err
			}
			inputRootDigest = d
			return nil
		})
	} else {
		d, err := digest.ComputeForMessage(&repb.Directory{}, c.digestFunction())
		if err != nil {
			return err
		}
		inputRootDigest = d
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	a := &repb.Action{
		CommandDigest:   commandDigest,
		InputRootDigest: inputRootDigest,
	}
	if c.Timeout != 0 {
		a.Timeout = durationpb.New(c.Timeout)
	}
	ad, err := cachetools.UploadProto(ctx, c.env.GetByteStreamClient(), c.InstanceName, c.digestFunction(), a)
	if err != nil {
		return err
	}
	c.actionDigest = ad
	return nil
}

// Start calls Upload if it has not already been called, then initiates an
// Execute request. It does not wait for the Execute stream to be completed.
func (c *Cmd) Start(ctx context.Context) error {
	if c.state >= starting {
		return nil
	}
	c.state = starting

	if c.actionDigest == nil {
		if err := c.Upload(ctx); err != nil {
			return err
		}
	}
	req := &repb.ExecuteRequest{
		InstanceName:    c.InstanceName,
		SkipCacheLookup: true,
		ActionDigest:    c.actionDigest,
		DigestFunction:  c.digestFunction(),
	}
	stream, err := c.env.GetRemoteExecutionClient().Execute(ctx, req)
	if err != nil {
		return err
	}
	c.stream = NewRetryingStream(ctx, c.env.GetRemoteExecutionClient(), stream, "")
	return nil
}

// Wait waits for command execution to complete.
func (c *Cmd) Wait(ctx context.Context) error {
	if c.state >= waiting {
		return nil
	}
	c.state = waiting

	defer func() {
		c.stream.CloseSend()
		c.stream = nil
	}()
	for {
		op, err := c.stream.Recv()
		if err != nil {
			return err
		}
		msg, err := UnpackOperation(op)
		if err != nil {
			return err
		}
		if msg.ExecuteResponse != nil {
			c.response = msg.ExecuteResponse
		}
		if msg.Operation.GetDone() {
			return msg.Err
		}
	}
}

func (c *Cmd) Run(ctx context.Context) error {
	if err := c.Start(ctx); err != nil {
		return err
	}
	return c.Wait(ctx)
}

// Result runs the command and returns the result. If the command has already
// been started, it waits for the existing execution to complete.
func (c *Cmd) Result(ctx context.Context) (*interfaces.CommandResult, error) {
	if c.commandResult != nil {
		return c.commandResult, nil
	}
	if err := c.Run(ctx); err != nil {
		return nil, err
	}
	res := c.response.GetResult()
	var stdout, stderr bytes.Buffer
	eg, egctx := errgroup.WithContext(ctx)
	if res.GetStdoutDigest() != nil {
		eg.Go(func() error {
			ctx := egctx
			rn := digest.NewResourceName(res.GetStdoutDigest(), c.InstanceName, rspb.CacheType_CAS, c.digestFunction())
			return cachetools.GetBlob(ctx, c.env.GetByteStreamClient(), rn, &stdout)
		})
	}
	if res.GetStderrDigest() != nil {
		eg.Go(func() error {
			ctx := egctx
			rn := digest.NewResourceName(res.GetStderrDigest(), c.InstanceName, rspb.CacheType_CAS, c.digestFunction())
			return cachetools.GetBlob(ctx, c.env.GetByteStreamClient(), rn, &stderr)
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	c.commandResult = &interfaces.CommandResult{
		ExitCode: int(res.GetExitCode()),
		Stdout:   stdout.Bytes(),
		Stderr:   stderr.Bytes(),
	}
	return c.commandResult, nil
}

func (c *Cmd) digestFunction() repb.DigestFunction_Value {
	if c.DigestFunction != 0 {
		return c.DigestFunction
	}
	return defaultDigestFunction
}

// RetryingStream implements a reliable operation stream.
//
// It keeps track of the operation name internally, and provides a Recv() func
// which re-establishes the stream transparently if the operation name has been
// established.
type RetryingStream struct {
	ctx    context.Context
	cancel context.CancelFunc
	client repb.ExecutionClient
	stream repb.Execution_ExecuteClient
	name   string
}

func NewRetryingStream(ctx context.Context, client repb.ExecutionClient, stream repb.Execution_ExecuteClient, name string) *RetryingStream {
	ctx, cancel := context.WithCancel(ctx)
	return &RetryingStream{
		ctx:    ctx,
		cancel: cancel,
		client: client,
		stream: stream,
		name:   name,
	}
}

// Name returns the operation name, if known.
func (s *RetryingStream) Name() string {
	return s.name
}

// Recv attempts to reliably return the next operation on the named stream.
//
// If the stream is disconnected and the operation name has been received, it
// will attempt to reconnect with WaitExecution.
func (s *RetryingStream) Recv() (*longrunning.Operation, error) {
	r := retry.DefaultWithContext(s.ctx)
	for {
		op, err := s.stream.Recv()
		if err == nil {
			if op.GetName() != "" {
				s.name = op.GetName()
			}
			return op, nil
		}
		if !status.IsUnavailableError(err) || s.name == "" {
			return nil, err
		}
		if !r.Next() {
			return nil, s.ctx.Err()
		}
		req := &repb.WaitExecutionRequest{Name: s.name}
		next, err := s.client.WaitExecution(s.ctx, req)
		if err != nil {
			return nil, err
		}
		s.stream.CloseSend()
		s.stream = next
	}
}

func (s *RetryingStream) CloseSend() error {
	var err error
	if s.stream != nil {
		err = s.stream.CloseSend()
		s.stream = nil
	}
	s.client = nil
	s.cancel()
	return err
}

// ExecuteOperation contains an operation along with its execution-specific
// payload.
type ExecuteOperation struct {
	*longrunning.Operation

	// ExecuteOperationMetadata contains any metadata unpacked from the
	// operation.
	ExecuteOperationMetadata *repb.ExecuteOperationMetadata
	// ExecuteResponse contains any response unpacked from the operation.
	ExecuteResponse *repb.ExecuteResponse
	// Err contains any error parsed from the ExecuteResponse status field.
	Err error
}

// UnpackOperation unmarshals all expected execution-specific fields from the
// given operationn.
func UnpackOperation(op *longrunning.Operation) (*ExecuteOperation, error) {
	msg := &ExecuteOperation{Operation: op}
	if op.GetResponse() != nil {
		msg.ExecuteResponse = &repb.ExecuteResponse{}
		if err := op.GetResponse().UnmarshalTo(msg.ExecuteResponse); err != nil {
			return nil, err
		}
	}
	if op.GetMetadata() != nil {
		msg.ExecuteOperationMetadata = &repb.ExecuteOperationMetadata{}
		if err := op.GetMetadata().UnmarshalTo(msg.ExecuteOperationMetadata); err != nil {
			return nil, err
		}
	}
	msg.Err = gstatus.FromProto(msg.ExecuteResponse.GetStatus()).Err()
	return msg, nil
}

// AssemblePlatform assembles a Platform proto from a list of NAME=VALUE pairs.
// If the same name is specified more than once, the last one wins. The entries
// are sorted by name, so that the platform is cache-friendly.
func AssemblePlatform(pairs ...string) (*repb.Platform, error) {
	m := map[string]string{}
	for _, s := range pairs {
		parts := strings.SplitN(s, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid NAME=VALUE pair %q", s)
		}
		m[parts[0]] = parts[1]
	}
	names := maps.Keys(m)
	sort.Strings(names)
	p := &repb.Platform{Properties: make([]*repb.Platform_Property, 0, len(names))}
	for _, name := range names {
		p.Properties = append(p.Properties, &repb.Platform_Property{
			Name:  name,
			Value: m[name],
		})
	}
	return p, nil
}

// AssembleEnvironmentVariables assembles a list of EnvironmentVariable protos
// from a list of NAME=VALUE pairs. If the same name is specified more than
// once, the last one wins. The entries are sorted by name, so that the
// environment variables are cache-friendly.
func AssembleEnvironmentVariables(pairs ...string) ([]*repb.Command_EnvironmentVariable, error) {
	m := map[string]string{}
	for _, pair := range pairs {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid environment variable")
		}
		m[parts[0]] = parts[1]
	}
	names := maps.Keys(m)
	sort.Strings(names)
	out := make([]*repb.Command_EnvironmentVariable, 0, len(m))
	for _, name := range names {
		out = append(out, &repb.Command_EnvironmentVariable{
			Name:  name,
			Value: m[name],
		})
	}
	return out, nil
}
