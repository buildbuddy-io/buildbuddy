// package rexec provides utility functions for remote execution clients.
package rexec

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
)

// MakeEnv assembles a list of EnvironmentVariable protos from a list of
// NAME=VALUE pairs. If the same name is specified more than once, the last one
// wins. The entries are sorted by name, so that the environment variables are
// cache-friendly.
func MakeEnv(pairs ...string) ([]*repb.Command_EnvironmentVariable, error) {
	m, err := parsePairs(pairs)
	if err != nil {
		return nil, fmt.Errorf("parse environment variables: %w", err)
	}
	names := slices.Sorted(maps.Keys(m))
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
		return nil, fmt.Errorf("parse platform properties: %w", err)
	}
	names := slices.Sorted(maps.Keys(m))
	p := &repb.Platform{Properties: make([]*repb.Platform_Property, 0, len(names))}
	for _, name := range names {
		p.Properties = append(p.Properties, &repb.Platform_Property{
			Name:  name,
			Value: m[name],
		})
	}
	return p, nil
}

// LookupEnv returns the value of the environment variable with the given name,
// or an empty string if the variable is not present. The second return value
// indicates whether the variable was found.
func LookupEnv(envs []*repb.Command_EnvironmentVariable, name string) (string, bool) {
	for _, e := range envs {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

// NormalizeCommand produces a canonicalized version of the given command
// that is suitable for caching, without altering the semantics of the command.
func NormalizeCommand(cmd *repb.Command) {
	if cmd == nil {
		return
	}
	cmd.EnvironmentVariables = normalizedEnv(cmd.EnvironmentVariables)
	if cmd.Platform != nil {
		cmd.Platform.Properties = normalizedPlatform(cmd.Platform.Properties)
	}
}

// normalizedEnv returns environment variables sorted alphabetically by name. If
// the same name is specified more than once in the original list, the last one
// wins.
func normalizedEnv(envs []*repb.Command_EnvironmentVariable) []*repb.Command_EnvironmentVariable {
	m := make(map[string]string, len(envs))
	for _, e := range envs {
		m[e.Name] = e.Value
	}
	names := slices.Sorted(maps.Keys(m))
	out := make([]*repb.Command_EnvironmentVariable, 0, len(m))
	for _, name := range names {
		out = append(out, &repb.Command_EnvironmentVariable{
			Name:  name,
			Value: m[name],
		})
	}
	return out
}

// normalizedPlatform returns platform properties sorted alphabetically by name.
// If the same name is specified more than once in the original list, the last
// one wins.
func normalizedPlatform(props []*repb.Platform_Property) []*repb.Platform_Property {
	m := make(map[string]string, len(props))
	for _, p := range props {
		m[p.Name] = p.Value
	}

	uniqueProps := make([]*repb.Platform_Property, 0, len(m))
	for k, v := range m {
		uniqueProps = append(uniqueProps, &repb.Platform_Property{
			Name:  k,
			Value: v,
		})
	}

	sort.Slice(uniqueProps, func(i, j int) bool {
		return uniqueProps[i].Name < uniqueProps[j].Name
	})
	return uniqueProps
}

// parsePairs parses a list of "NAME=VALUE" pairs into a map. If the same NAME
// appears more than once, the last one wins.
func parsePairs(pairs []string) (map[string]string, error) {
	m := map[string]string{}
	for _, pair := range pairs {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			return nil, status.InvalidArgumentErrorf("invalid syntax %q (expected NAME=VALUE)", pair)
		}
		m[parts[0]] = parts[1]
	}
	return m, nil
}

// Prepare uploads an Action and all of its dependencies to cache, populating
// uploaded digests into the Action.
//
// The `Action.command_digest` field is optional. If it is set, the command
// digest is expected to already have been uploaded. If it is not set, the given
// Command is uploaded to cache, and the resulting command digest is set on the
// Action.
//
// The `Action.input_root_digest` field is optional. If it is set, the input
// root digest is expected to already have been uploaded. If it is not set, the
// given local inputRootDir path is walked and recursively uploaded to cache,
// and the resulting root directory digest is set on the Action. An empty string
// for inputRootDir is treated as an empty directory, meaning the action will
// run from an empty input root.
//
// TODO: The REAPI spec says platform properties and environment variables
// "MUST" be sorted alphabetically by name. We should either automatically
// normalize here, or return an error if input is not normalized.
func Prepare(ctx context.Context, env environment.Env, instanceName string, digestFunction repb.DigestFunction_Value, action *repb.Action, cmd *repb.Command, inputRootDir string) (*rspb.ResourceName, error) {
	var commandDigest, inputRootDigest *repb.Digest
	eg, egctx := errgroup.WithContext(ctx)
	if action.CommandDigest != nil && cmd != nil {
		return nil, status.InvalidArgumentErrorf("cannot specify both an Action.command_digest and a Command")
	}
	if action.CommandDigest == nil && cmd == nil {
		return nil, status.InvalidArgumentErrorf("must specify either Action.command_digest or a Command")
	}
	if inputRootDir != "" && action.InputRootDigest != nil {
		return nil, status.InvalidArgumentErrorf("cannot specify both an input root directory and Action.input_root_digest")
	}
	if cmd != nil {
		eg.Go(func() error {
			d, err := cachetools.UploadProto(egctx, env.GetByteStreamClient(), instanceName, digestFunction, cmd)
			if err != nil {
				return err
			}
			commandDigest = d
			return nil
		})
	}
	if inputRootDir != "" {
		eg.Go(func() error {
			d, _, err := cachetools.UploadDirectoryToCAS(egctx, env, instanceName, digestFunction, inputRootDir)
			if err != nil {
				return err
			}
			inputRootDigest = d
			return nil
		})
	} else if action.InputRootDigest == nil {
		// If running an action without an input root, set it to the digest of
		// an empty directory, since the input_root_digest field is required.
		d, err := digest.Compute(bytes.NewReader(nil), digestFunction)
		if err != nil {
			return nil, err
		}
		inputRootDigest = d
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	if commandDigest != nil {
		action.CommandDigest = commandDigest
	}
	if inputRootDigest != nil {
		action.InputRootDigest = inputRootDigest
	}
	actionDigest, err := cachetools.UploadProto(ctx, env.GetByteStreamClient(), instanceName, digestFunction, action)
	if err != nil {
		return nil, err
	}
	actionResourceName := digest.NewCASResourceName(actionDigest, instanceName, digestFunction).ToProto()
	return actionResourceName, nil
}

// StartOption applies an option to the ExecuteRequest built by Start.
type StartOption func(*repb.ExecuteRequest)

// WithSkipCacheLookup configures whether the Execute request should bypass the
// Action Cache lookup before execution.
func WithSkipCacheLookup(skipCacheLookup bool) StartOption {
	return func(req *repb.ExecuteRequest) {
		req.SkipCacheLookup = skipCacheLookup
	}
}

// Start begins an Execute stream for the given remote action.
//
// By default it bypasses Action Cache lookups before execution. Callers may
// override request fields using StartOption values.
func Start(ctx context.Context, env environment.Env, actionResourceName *rspb.ResourceName, opts ...StartOption) (*RetryingStream, error) {
	req := &repb.ExecuteRequest{
		InstanceName:    actionResourceName.GetInstanceName(),
		ActionDigest:    actionResourceName.GetDigest(),
		DigestFunction:  actionResourceName.GetDigestFunction(),
		SkipCacheLookup: true,
	}
	for _, opt := range opts {
		opt(req)
	}
	client := env.GetRemoteExecutionClient()
	s := NewRetryingStream(ctx, client, nil, "")
	s.executeReq = req
	stream, err := client.Execute(s.ctx, req)
	if err == nil {
		s.stream = stream
		return s, nil
	}
	// Start the execution again if the error is retryable, the same way Recv
	// does when a stream fails before the server sends the operation name.
	if s.prepareRetry(err) {
		err = s.reconnect(err)
	}
	if err != nil {
		s.CloseSend()
		return nil, err
	}
	return s, nil
}

// Wait waits for command execution to complete, and returns the COMPLETE stage
// operation response.
func Wait(stream *RetryingStream) (*Response, error) {
	for {
		op, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		if op.Done {
			return op, nil
		}
	}
}

// Result runs the command and returns the result. If the command has already
// been started, it waits for the existing execution to complete.
func GetResult(ctx context.Context, env environment.Env, instanceName string, digestFunction repb.DigestFunction_Value, res *repb.ActionResult) (*interfaces.CommandResult, error) {
	stdout, stderr, err := getStdoutAndStderr(ctx, env.GetByteStreamClient(), instanceName, digestFunction, res)
	if err != nil {
		return nil, err
	}
	return &interfaces.CommandResult{
		ExitCode: int(res.GetExitCode()),
		Stdout:   stdout,
		Stderr:   stderr,
	}, nil
}

// RetryingStream implements a reliable operation stream.
//
// It keeps track of the operation name internally, and provides a Recv() func
// which re-establishes the stream transparently. Once the operation name is
// known, it re-attaches with WaitExecution. If it has the ExecuteRequest, it
// can also start the execution again when the server never acknowledged it,
// lost it, or finished it with a transient error.
type RetryingStream struct {
	ctx    context.Context
	cancel context.CancelFunc
	client repb.ExecutionClient
	// The request that started the execution, if known.
	executeReq *repb.ExecuteRequest
	// Backs off between reconnects and limits how many we make in a row. Recv
	// resets it whenever the stream delivers an operation.
	retry  *retry.Retry
	stream repb.Execution_ExecuteClient
	name   string
}

func NewRetryingStream(ctx context.Context, client repb.ExecutionClient, stream repb.Execution_ExecuteClient, name string) *RetryingStream {
	ctx, cancel := context.WithCancel(ctx)
	s := &RetryingStream{
		ctx:    ctx,
		cancel: cancel,
		client: client,
		stream: stream,
		name:   name,
		retry: retry.New(ctx, &retry.Options{
			InitialBackoff: 100 * time.Millisecond,
			MaxBackoff:     5 * time.Second,
			Multiplier:     2,
			MaxRetries:     5,
		}),
	}
	s.resetRetries()
	return s
}

// resetRetries restarts the retry limit and backoff. The retrier's first
// attempt doesn't wait or count as a retry, and the current stream is that
// attempt, so we use it up here.
func (s *RetryingStream) resetRetries() {
	s.retry.Reset()
	s.retry.Next()
}

// Name returns the operation name, if known.
func (s *RetryingStream) Name() string {
	return s.name
}

// Recv attempts to reliably return the next operation on the named stream.
//
// If the stream fails with a transient error, it reconnects as described on
// RetryingStream.
func (s *RetryingStream) Recv() (*Response, error) {
	for {
		op, err := s.stream.Recv()
		if err == nil {
			if op.GetName() != "" {
				s.name = op.GetName()
			}
			// Executions can run for hours, so only limit retries in a row, and
			// start over whenever the stream makes progress.
			s.resetRetries()
			rsp, err := UnpackOperation(op)
			if err != nil {
				return nil, err
			}
			// If the execution finished with a transient error, start it again.
			// If that fails, return the finished execution's response.
			if rsp.GetDone() && s.executeReq != nil && isRetryableExecutionFailure(s.ctx, rsp) {
				s.name = ""
				if s.reconnect(rsp.Err) == nil {
					continue
				}
			}
			return rsp, nil
		}
		if err == io.EOF && s.name != "" {
			// Servers shouldn't end the stream before the operation is done,
			// but proxies in between might, so re-attach without using up a
			// retry.
			next, waitErr := s.client.WaitExecution(s.ctx, &repb.WaitExecutionRequest{Name: s.name})
			if waitErr == nil {
				s.stream.CloseSend()
				s.stream = next
				continue
			}
			err = waitErr
		}
		if !s.prepareRetry(err) {
			return nil, err
		}
		if err := s.reconnect(err); err != nil {
			return nil, err
		}
	}
}

// reconnect opens a new stream after the current one failed with err. Once the
// operation name is known, it re-attaches with WaitExecution, and otherwise it
// retries the execution with Execute, which may occasionally trigger a
// duplicate execution if the server had already scheduled the prior execution
// attempt without the client receiving the reply. This matches Bazel's
// behavior:
// https://github.com/bazelbuild/bazel/blob/9551cddb4b3f271bbacfaebab0ccc1ab5a090807/src/main/java/com/google/devtools/build/lib/remote/GrpcRemoteExecutor.java#L111-L117
//
// It retries transient errors from opening the stream too, and returns the
// error to give up with once retries run out.
func (s *RetryingStream) reconnect(err error) error {
	for {
		// Next returns false either because our context is done or because
		// retries ran out.
		if !s.retry.Next() {
			if s.ctx.Err() != nil {
				return s.ctx.Err()
			}
			return err
		}
		var next repb.Execution_ExecuteClient
		var openErr error
		if s.name != "" {
			next, openErr = s.client.WaitExecution(s.ctx, &repb.WaitExecutionRequest{Name: s.name})
		} else {
			next, openErr = s.client.Execute(s.ctx, s.executeReq)
		}
		if openErr == nil {
			if s.stream != nil {
				s.stream.CloseSend()
			}
			s.stream = next
			return nil
		}
		if !s.prepareRetry(openErr) {
			return openErr
		}
		err = openErr
	}
}

// prepareRetry returns whether to open a new stream after err. If the server
// lost the operation, it forgets the operation name, so that the next stream
// starts the execution again.
func (s *RetryingStream) prepareRetry(err error) bool {
	if s.ctx.Err() != nil {
		return false
	}
	if gstatus.Code(err) == codes.NotFound && s.executeReq != nil {
		s.name = ""
		return true
	}
	// Without the operation name, we can only start the execution again.
	return isTransientExecutionError(s.ctx, err) && (s.name != "" || s.executeReq != nil)
}

// isRetryableExecutionFailure returns whether starting a finished execution
// again might succeed. That's the case for transient errors, and for NotFound,
// which the server returns when it lost the execution. Action timeouts don't
// count, since the action would most likely time out again.
func isRetryableExecutionFailure(ctx context.Context, rsp *Response) bool {
	err := rsp.Err
	if st := rsp.GetError(); st != nil {
		err = gstatus.FromProto(st).Err()
	} else if rsp.ExecuteResponse.GetStatus().GetCode() == int32(codes.DeadlineExceeded) {
		return false
	}
	if err == nil || ctx.Err() != nil {
		return false
	}
	return gstatus.Code(err) == codes.NotFound || isTransientExecutionError(ctx, err)
}

// isTransientExecutionError returns whether a stream error returned by
// Execute or WaitExecution is transient and should be retried.
// Matches Bazel's behavior here:
// https://github.com/bazelbuild/bazel/blob/9551cddb4b3f271bbacfaebab0ccc1ab5a090807/src/main/java/com/google/devtools/build/lib/remote/RemoteRetrier.java#L69-L76
func isTransientExecutionError(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	st, ok := gstatus.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.Unknown, codes.DeadlineExceeded, codes.Aborted, codes.Internal, codes.Unavailable, codes.ResourceExhausted, codes.Canceled:
		return true
	default:
		return false
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

// Response contains an operation along with its execution-specific payload.
type Response struct {
	*longrunningpb.Operation

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
func UnpackOperation(op *longrunningpb.Operation) (*Response, error) {
	msg := &Response{Operation: op}
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

// ExecutionLogs contains fetched stdout/stderr and server logs for an
// ExecuteResponse.
type ExecutionLogs struct {
	// Stdout contains command stdout bytes, either inline or fetched by digest.
	Stdout []byte
	// Stderr contains command stderr bytes, either inline or fetched by digest.
	Stderr []byte
	// ServerLogs contains fetched server log contents keyed by log name.
	ServerLogs map[string][]byte
}

// GetExecutionLogs fetches stdout/stderr and server log contents referenced by
// the given ExecuteResponse.
func GetExecutionLogs(ctx context.Context, bsClient bspb.ByteStreamClient, instanceName string, digestFunction repb.DigestFunction_Value, executeResponse *repb.ExecuteResponse) (*ExecutionLogs, error) {
	details := &ExecutionLogs{}
	result := executeResponse.GetResult()
	if result != nil {
		stdout, stderr, err := getStdoutAndStderr(ctx, bsClient, instanceName, digestFunction, result)
		if err != nil {
			return nil, fmt.Errorf("get stdout and stderr: %w", err)
		}
		details.Stdout = stdout
		details.Stderr = stderr
	}

	serverLogs := executeResponse.GetServerLogs()
	if len(serverLogs) == 0 {
		return details, nil
	}

	out := make(map[string][]byte, len(serverLogs))
	var mu sync.Mutex
	eg, egctx := errgroup.WithContext(ctx)
	for name, logFile := range serverLogs {
		d := logFile.GetDigest()
		if d == nil {
			continue
		}
		eg.Go(func() error {
			data, err := getBlob(egctx, bsClient, instanceName, digestFunction, d)
			if err != nil {
				return fmt.Errorf("get server log %q: %w", name, err)
			}
			mu.Lock()
			out[name] = data
			mu.Unlock()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	if len(out) > 0 {
		details.ServerLogs = out
	}
	return details, nil
}

// GetCachedExecuteResponse fetches an ExecuteResponse from action cache, using
// the execution ID as the lookup key. This is BuildBuddy-specific; it contains
// the full response that the executor reported back to the scheduler. The
// response returned to the execution client contains a subset of these fields
// (for example, some heavyweight info such as executor profiles aren't sent
// back to clients, but are available in the cached execute response).
func GetCachedExecuteResponse(ctx context.Context, acClient repb.ActionCacheClient, executionID string) (*repb.ExecuteResponse, error) {
	canonicalExecutionID := strings.TrimPrefix(executionID, "/")
	uploadResourceName, err := digest.ParseUploadResourceName(canonicalExecutionID)
	if err != nil {
		return nil, fmt.Errorf("parse execution ID as upload resource name: %w", err)
	}

	lookupExecutionIDs := []string{executionID}
	if canonicalExecutionID != executionID {
		lookupExecutionIDs = append(lookupExecutionIDs, canonicalExecutionID)
	}

	var actionResult *repb.ActionResult
	for i, lookupExecutionID := range lookupExecutionIDs {
		executeResponseDigest, err := digest.Compute(strings.NewReader(lookupExecutionID), uploadResourceName.GetDigestFunction())
		if err != nil {
			return nil, fmt.Errorf("compute execute response digest: %w", err)
		}
		acResourceName := digest.NewACResourceName(executeResponseDigest, uploadResourceName.GetInstanceName(), uploadResourceName.GetDigestFunction())
		actionResult, err = acClient.GetActionResult(ctx, &repb.GetActionResultRequest{
			ActionDigest:        acResourceName.GetDigest(),
			InstanceName:        acResourceName.GetInstanceName(),
			DigestFunction:      acResourceName.GetDigestFunction(),
			IncludeTimelineData: true,
		})
		if err == nil {
			break
		}
		if gstatus.Code(err) == codes.NotFound && i < len(lookupExecutionIDs)-1 {
			continue
		}
		return nil, fmt.Errorf("get action result: %w", err)
	}
	if len(actionResult.GetStdoutRaw()) == 0 {
		return nil, fmt.Errorf("cached action result did not include inline ExecuteResponse")
	}

	executeResponse := &repb.ExecuteResponse{}
	if err := proto.Unmarshal(actionResult.GetStdoutRaw(), executeResponse); err != nil {
		return nil, fmt.Errorf("unmarshal execute response: %w", err)
	}
	return executeResponse, nil
}

func getStdoutAndStderr(ctx context.Context, bsClient bspb.ByteStreamClient, instanceName string, digestFunction repb.DigestFunction_Value, result *repb.ActionResult) ([]byte, []byte, error) {
	stdout := append([]byte(nil), result.GetStdoutRaw()...)
	stderr := append([]byte(nil), result.GetStderrRaw()...)

	eg, egctx := errgroup.WithContext(ctx)
	if len(stdout) == 0 && result.GetStdoutDigest() != nil {
		eg.Go(func() error {
			data, err := getBlob(egctx, bsClient, instanceName, digestFunction, result.GetStdoutDigest())
			if err != nil {
				return fmt.Errorf("get stdout: %w", err)
			}
			stdout = data
			return nil
		})
	}
	if len(stderr) == 0 && result.GetStderrDigest() != nil {
		eg.Go(func() error {
			data, err := getBlob(egctx, bsClient, instanceName, digestFunction, result.GetStderrDigest())
			if err != nil {
				return fmt.Errorf("get stderr: %w", err)
			}
			stderr = data
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}
	return stdout, stderr, nil
}

func getBlob(ctx context.Context, bsClient bspb.ByteStreamClient, instanceName string, digestFunction repb.DigestFunction_Value, d *repb.Digest) ([]byte, error) {
	if bsClient == nil {
		return nil, status.FailedPreconditionError("ByteStreamClient not configured")
	}
	var buf bytes.Buffer
	rn := digest.NewCASResourceName(d, instanceName, digestFunction)
	if err := cachetools.GetBlob(ctx, bsClient, rn, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// FindFirstAuxiliaryMetadata searches for the first auxiliary metadata entry
// with a type matching the full name of the given proto message descriptor. If
// one is found, it unmarshals the type into the given message.
// It returns whether the type was found as well as whether there was an error
// unmarshaling.
func FindFirstAuxiliaryMetadata(md *repb.ExecutedActionMetadata, pb proto.Message) (ok bool, err error) {
	typeURL := "type.googleapis.com/" + string(pb.ProtoReflect().Descriptor().FullName())
	for _, m := range md.GetAuxiliaryMetadata() {
		if m.TypeUrl == typeURL {
			if err := m.UnmarshalTo(pb); err != nil {
				return false, err
			}
			return true, nil
		}
	}
	return false, nil
}

// Retryable returns false if the error is a configuration error
// that will persist, even despite retries.
func Retryable(err error) bool {
	taskMisconfigured := status.IsInvalidArgumentError(err) ||
		status.IsFailedPreconditionError(err) ||
		status.IsUnauthenticatedError(err)
	return !taskMisconfigured
}
