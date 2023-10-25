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
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/durationpb"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
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
	stream        *operation.RetryingStream
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
		c.stream.Close()
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

	p, err := platform.FromPairs(c.Platform...)
	if err != nil {
		return err
	}
	var envVars []*repb.Command_EnvironmentVariable
	for _, kv := range c.Env {
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid environment variable")
		}
		envVars = append(envVars, &repb.Command_EnvironmentVariable{
			Name:  parts[0],
			Value: parts[1],
		})
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
	c.stream = operation.NewRetryingStream(ctx, c.env.GetRemoteExecutionClient(), stream, "")
	return nil
}

// Wait waits for command execution to complete.
func (c *Cmd) Wait(ctx context.Context) error {
	if c.state >= waiting {
		return nil
	}
	c.state = waiting

	defer func() {
		c.stream.Close()
		c.stream = nil
	}()
	for {
		op, err := c.stream.Recv()
		if err != nil {
			return err
		}
		msg, err := operation.Unpack(op)
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
