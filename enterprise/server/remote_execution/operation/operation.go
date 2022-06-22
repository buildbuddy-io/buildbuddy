package operation

import (
	"context"
	"encoding/base64"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gstatus "google.golang.org/grpc/status"
)

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
	op, err := Assemble(stage, name, d, &repb.ExecuteResponse{
		Status: gstatus.Convert(status).Proto(),
	})
	return op, err
}

type StreamLike interface {
	Context() context.Context
	Send(*longrunning.Operation) error
}

type StateChangeFunc func(stage repb.ExecutionStage_Value, execResponse *repb.ExecuteResponse) error
type FinishWithErrorFunc func(finalErr error) error

func GetStateChangeFunc(stream StreamLike, taskID string, adInstanceDigest *digest.ResourceName) StateChangeFunc {
	return func(stage repb.ExecutionStage_Value, execResponse *repb.ExecuteResponse) error {
		op, err := Assemble(stage, taskID, adInstanceDigest, execResponse)
		if err != nil {
			return status.InternalErrorf("Error updating state of %q: %s", taskID, err)
		}

		select {
		case <-stream.Context().Done():
			log.Warningf("Attempted state change on %q but context is done.", taskID)
			return status.UnavailableErrorf("Context cancelled: %s", stream.Context().Err())
		default:
			return stream.Send(op)
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
			return err
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
	rsp := &repb.ExecuteResponse{Status: gstatus.Convert(err).Proto()}
	if ar != nil {
		rsp.Result = ar
	}
	return rsp
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
