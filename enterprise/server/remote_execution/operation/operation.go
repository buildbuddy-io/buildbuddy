package operation

import (
	"context"
	"encoding/base64"
	"io"
	"net/url"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/protobuf/types/known/anypb"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gstatus "google.golang.org/grpc/status"
)

const (
	// Timeout for retrying a PublishOperation stream.
	reconnectTimeout = 5 * time.Second
)

// retryingClient works like a PublishOperationClient but transparently
// re-connects the stream when disconnected. The retryingClient does not re-dial
// the backend; instead it depends on the client connection being terminated by
// an L7 proxy.
type retryingClient struct {
	ctx          context.Context
	client       repb.ExecutionClient
	clientStream repb.Execution_PublishOperationClient
	lastMsg      *longrunning.Operation
}

// Publish begins a PublishOperation stream and transparently reconnects the
// stream if disconnected. After a disconnect (either in Send or CloseAndRecv),
// it will re-publish the last sent message if applicable to ensure that the
// server has acknowledged it.
func Publish(ctx context.Context, client repb.ExecutionClient) (*retryingClient, error) {
	clientStream, err := client.PublishOperation(ctx)
	if err != nil {
		return nil, err
	}
	return &retryingClient{
		ctx:          ctx,
		client:       client,
		clientStream: clientStream,
	}, nil
}

func (c *retryingClient) Context() context.Context {
	return c.ctx
}

func (c *retryingClient) Send(msg *longrunning.Operation) error {
	// If CloseAndRecv fails, this message isn't guaranteed to be ack'd by the
	// server, so when retrying CloseAndRecv we need to re-send this message
	// first to ensure it is ack'd.
	c.lastMsg = msg
	return c.sendWithRetry(msg)
}

func (c *retryingClient) sendWithRetry(msg *longrunning.Operation) error {
	var lastErr error
	retryCtx, cancel := context.WithTimeout(c.ctx, reconnectTimeout)
	defer cancel()
	r := retry.DefaultWithContext(retryCtx)
	for r.Next() {
		err := c.clientStream.Send(msg)
		if err == nil {
			return nil
		}
		if err != io.EOF {
			return err
		}
		lastErr = err
		log.CtxInfof(c.ctx, "PublishOperation stream disconnected; attempting to reconnect.")
		// EOF means we got disconnected; reconnect and retry.
		if err := c.reconnect(retryCtx); err != nil {
			return status.WrapError(err, "failed to reconnect PublishOperation stream")
		}
	}
	if lastErr != nil {
		return lastErr
	}
	// Retry loop didn't even execute once; this should only happen if
	// there is a ctx error. Return that error.
	if retryCtx.Err() != nil {
		return retryCtx.Err()
	}
	// Should never happen, but make sure we still return an error in this case.
	return status.UnknownError("Send: unknown error")
}

func (s *retryingClient) reconnect(retryCtx context.Context) error {
	r := retry.DefaultWithContext(retryCtx)
	var lastErr error
	for r.Next() {
		// Note, we don't use the retryCtx here because it has a timeout, and we
		// don't want this timeout to affect the RPC once it succeeds.
		clientStream, err := s.client.PublishOperation(s.ctx)
		if err != nil {
			lastErr = err
			continue
		}
		log.CtxInfof(s.ctx, "Successfully reconnected PublishOperation stream.")
		s.clientStream = clientStream
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	// Retry loop didn't even execute once; this should only happen if
	// there is a ctx error. Return that error.
	if retryCtx.Err() != nil {
		return retryCtx.Err()
	}
	// Should never happen, but make sure we still return an error in this case.
	return status.UnknownError("reconnect: unknown error")
}

func (c *retryingClient) CloseAndRecv() (*repb.PublishOperationResponse, error) {
	var lastErr error
	retryCtx, cancel := context.WithTimeout(c.ctx, reconnectTimeout)
	defer cancel()
	r := retry.DefaultWithContext(retryCtx)
	for r.Next() {
		res, err := c.clientStream.CloseAndRecv()
		if err == nil {
			return res, nil
		}
		if err != io.EOF {
			return nil, err
		}
		lastErr = err
		log.CtxInfof(c.ctx, "PublishOperation stream disconnected; attempting to reconnect.")
		// Stream is broken; reconnect and retry. If this fails, return the
		// original error.
		if err := c.reconnect(retryCtx); err != nil {
			log.CtxWarningf(c.ctx, "Failed to reconnect operation stream: %s", err)
			break
		}
		// Since CloseAndRecv failed, the server isn't guaranteed to have gotten
		// our last published message, so publish it again. But if that fails,
		// just return the original error.
		if c.lastMsg == nil {
			continue
		}
		if err := c.sendWithRetry(c.lastMsg); err != nil {
			log.CtxWarningf(c.ctx, "Failed to retry un-acknowledged operation update: %s", err)
			break
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	// Retry loop didn't even execute once; this should only happen if
	// there is a ctx error. Return that error.
	if retryCtx.Err() != nil {
		return nil, retryCtx.Err()
	}
	// Should never happen, but make sure we still return an error in this case.
	return nil, status.UnknownError("CloseAndRecv: unknown error")
}

func Assemble(stage repb.ExecutionStage_Value, name string, r *digest.ResourceName, er *repb.ExecuteResponse) (*longrunning.Operation, error) {
	if r == nil || er == nil {
		return nil, status.FailedPreconditionError("digest or execute response are both required to assemble operation")
	}
	metadata, err := anypb.New(&repb.ExecuteOperationMetadata{
		Stage:        stage,
		ActionDigest: r.GetDigest(),
	})
	if err != nil {
		return nil, err
	}
	operation := &longrunning.Operation{
		Name:     name,
		Metadata: metadata,
	}
	result, err := anypb.New(er)
	if err != nil {
		return nil, err
	}
	operation.Result = &longrunning.Operation_Response{Response: result}

	if stage == repb.ExecutionStage_COMPLETED {
		operation.Done = true
	}
	return operation, nil
}

func AssembleFailed(stage repb.ExecutionStage_Value, name string, d *digest.ResourceName, status error) (*longrunning.Operation, error) {
	return Assemble(stage, name, d, ErrorResponse(status))
}

func ErrorResponse(err error) *repb.ExecuteResponse {
	return &repb.ExecuteResponse{
		Status: gstatus.Convert(err).Proto(),
	}
}

type StreamLike interface {
	Context() context.Context
	Send(*longrunning.Operation) error
}

type StateChangeFunc func(stage repb.ExecutionStage_Value, execResponse *repb.ExecuteResponse) error
type FinishWithErrorFunc func(finalErr error) error

func GetStateChangeFunc(stream StreamLike, taskID string, adInstanceDigest *digest.ResourceName) StateChangeFunc {
	return func(stage repb.ExecutionStage_Value, execResponse *repb.ExecuteResponse) error {
		if stage == repb.ExecutionStage_COMPLETED {
			if target, err := flagutil.GetDereferencedValue[string]("executor.app_target"); err == nil {
				if u, err := url.Parse(target); err == nil && u.Hostname() == "cloud.buildbuddy.io" {
					if execResponse.GetMessage() != "" {
						execResponse.Message = execResponse.GetMessage() + "\n"
					}
					execResponse.Message = (execResponse.GetMessage() +
						"The build used the old BuildBuddy endpoint, cloud.buildbuddy.io. " +
						"Migrate `executor.app_target` to remote.buildbuddy.io for " +
						"improved performance.")
				}
			}
		}
		op, err := Assemble(stage, taskID, adInstanceDigest, execResponse)
		if err != nil {
			return status.InternalErrorf("Error updating state of %q: %s", taskID, err)
		}

		select {
		case <-stream.Context().Done():
			log.Warningf("Attempted state change on %q but context is done.", taskID)
			return status.UnavailableErrorf("Context cancelled: %s", stream.Context().Err())
		default:
			if err := stream.Send(op); err != nil {
				return status.WrapError(err, "failed to send execution status update")
			}
			return nil
		}
	}
}

func PublishOperationDone(stream StreamLike, taskID string, adInstanceDigest *digest.ResourceName, finalErr error) error {
	stage := repb.ExecutionStage_COMPLETED
	op, err := AssembleFailed(stage, taskID, adInstanceDigest, finalErr)
	if err != nil {
		return err
	}

	select {
	case <-stream.Context().Done():
		log.Warningf("Attempted finish with err on %q but context is done.", taskID)
		return status.UnavailableErrorf("Context cancelled: %s", stream.Context().Err())
	default:
		if err := stream.Send(op); err != nil {
			log.Errorf("Error sending operation %+v on stream", op)
			return status.WrapError(err, "failed to send execution status update")
		}
	}
	return nil
}

// ExecuteResponseWithCachedResult returns an ExecuteResponse for an action
// result served from cache.
func ExecuteResponseWithCachedResult(ar *repb.ActionResult) *repb.ExecuteResponse {
	return ExecuteResponseWithResult(ar, nil /*=err*/)
}

// ExecuteResponseWithResult returns an ExecuteResponse for an action result
// produced by actually executing an action. The given summary pertains to the
// execution, and the error is any pertinent error encountered during execution.
// If a non-nil error is provided, an action result (incomplete or partial) may
// still be provided, and clients are expected to handle this case properly.
func ExecuteResponseWithResult(ar *repb.ActionResult, err error) *repb.ExecuteResponse {
	return &repb.ExecuteResponse{
		Status: gstatus.Convert(err).Proto(),
		Result: ar,
	}
}

func InProgressExecuteResponse() *repb.ExecuteResponse {
	return ExecuteResponseWithResult(nil /*=result*/, nil /*=error*/)
}

func ExtractStage(op *longrunning.Operation) repb.ExecutionStage_Value {
	md := &repb.ExecuteOperationMetadata{}
	if err := op.GetMetadata().UnmarshalTo(md); err != nil {
		return repb.ExecutionStage_UNKNOWN
	}
	return md.GetStage()
}

func ExtractExecuteResponse(op *longrunning.Operation) *repb.ExecuteResponse {
	er := &repb.ExecuteResponse{}
	if result := op.GetResult(); result != nil {
		if response, ok := result.(*longrunning.Operation_Response); ok {
			if err := response.Response.UnmarshalTo(er); err == nil {
				return er
			}
		}
	}
	return nil
}

func Decode(serializedOperation string) (*longrunning.Operation, error) {
	op := &longrunning.Operation{}
	data, err := base64.StdEncoding.DecodeString(serializedOperation)
	if err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(data, op); err != nil {
		return nil, err
	}
	return op, nil
}
