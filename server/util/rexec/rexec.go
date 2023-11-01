// package rexec provides utility functions for remote execution clients.
package rexec

import (
	"bytes"
	"context"
	"sort"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"google.golang.org/genproto/googleapis/longrunning"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	gstatus "google.golang.org/grpc/status"
)

// MakeEnv assembles a list of EnvironmentVariable protos from a list of
// NAME=VALUE pairs. If the same name is specified more than once, the last one
// wins. The entries are sorted by name, so that the environment variables are
// cache-friendly.
func MakeEnv(pairs ...string) ([]*repb.Command_EnvironmentVariable, error) {
	m, err := parsePairs(pairs)
	if err != nil {
		return nil, err
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

// MakePlatform assembles a Platform proto from a list of NAME=VALUE pairs. If
// the same name is specified more than once, the last one wins. The entries are
// sorted by name, so that the platform is cache-friendly.
func MakePlatform(pairs ...string) (*repb.Platform, error) {
	m, err := parsePairs(pairs)
	if err != nil {
		return nil, err
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

// parsePairs parses a list of "NAME=VALUE" pairs into a map. If the same NAME
// appears more than once, the last one wins.
func parsePairs(pairs []string) (map[string]string, error) {
	m := map[string]string{}
	for _, pair := range pairs {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			return nil, status.InvalidArgumentErrorf("invalid environment variable %q (expected NAME=VALUE)", pair)
		}
		m[parts[0]] = parts[1]
	}
	return m, nil
}

// Prepare transfers the given Command and local input root directory to cache,
// and populates the resulting digests into the given Action. An empty string
// for input root means that an empty directory will be used as the input root.
// A resource name pointing to the remote Action is returned.
func Prepare(ctx context.Context, env environment.Env, instanceName string, digestFunction repb.DigestFunction_Value, action *repb.Action, cmd *repb.Command, inputRootDir string) (*rspb.ResourceName, error) {
	var commandDigest, inputRootDigest *repb.Digest
	eg, egctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		d, err := cachetools.UploadProto(egctx, env.GetByteStreamClient(), instanceName, digestFunction, cmd)
		if err != nil {
			return err
		}
		commandDigest = d
		return nil
	})
	if inputRootDir != "" {
		eg.Go(func() error {
			d, _, err := cachetools.UploadDirectoryToCAS(egctx, env, instanceName, digestFunction, inputRootDir)
			if err != nil {
				return err
			}
			inputRootDigest = d
			return nil
		})
	} else {
		d, err := digest.Compute(bytes.NewReader(nil), digestFunction)
		if err != nil {
			return nil, err
		}
		inputRootDigest = d
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	action.CommandDigest = commandDigest
	action.InputRootDigest = inputRootDigest
	actionDigest, err := cachetools.UploadProto(ctx, env.GetByteStreamClient(), instanceName, digestFunction, action)
	if err != nil {
		return nil, err
	}
	actionResourceName := digest.NewResourceName(actionDigest, instanceName, rspb.CacheType_CAS, digestFunction).ToProto()
	return actionResourceName, nil
}

// Start begins an Execute stream for the given remote action.
func Start(ctx context.Context, env environment.Env, actionResourceName *rspb.ResourceName) (*RetryingStream, error) {
	req := &repb.ExecuteRequest{
		InstanceName:    actionResourceName.GetInstanceName(),
		ActionDigest:    actionResourceName.GetDigest(),
		DigestFunction:  actionResourceName.GetDigestFunction(),
		SkipCacheLookup: true,
	}
	stream, err := env.GetRemoteExecutionClient().Execute(ctx, req)
	if err != nil {
		return nil, err
	}
	return NewRetryingStream(ctx, env.GetRemoteExecutionClient(), stream, ""), nil
}

// Wait waits for command execution to complete, and returns the COMPLETE stage
// operation response.
func Wait(stream *RetryingStream) (*ExecuteOperation, error) {
	for {
		op, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		msg, err := unpackOperation(op)
		if err != nil {
			return nil, err
		}
		if msg.Operation.GetDone() {
			return msg, nil
		}
	}
}

// Result runs the command and returns the result. If the command has already
// been started, it waits for the existing execution to complete.
func GetResult(ctx context.Context, env environment.Env, instanceName string, digestFunction repb.DigestFunction_Value, res *repb.ActionResult) (*interfaces.CommandResult, error) {
	var stdout, stderr bytes.Buffer
	eg, egctx := errgroup.WithContext(ctx)
	if res.GetStdoutDigest() != nil {
		eg.Go(func() error {
			rn := digest.NewResourceName(res.GetStdoutDigest(), instanceName, rspb.CacheType_CAS, digestFunction)
			return cachetools.GetBlob(egctx, env.GetByteStreamClient(), rn, &stdout)
		})
	}
	if res.GetStderrDigest() != nil {
		eg.Go(func() error {
			rn := digest.NewResourceName(res.GetStderrDigest(), instanceName, rspb.CacheType_CAS, digestFunction)
			return cachetools.GetBlob(egctx, env.GetByteStreamClient(), rn, &stderr)
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return &interfaces.CommandResult{
		ExitCode: int(res.GetExitCode()),
		Stdout:   stdout.Bytes(),
		Stderr:   stderr.Bytes(),
	}, nil
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

// unpackOperation unmarshals all expected execution-specific fields from the
// given operationn.
func unpackOperation(op *longrunning.Operation) (*ExecuteOperation, error) {
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
