package operation

import (
	"context"
	"encoding/base64"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc/codes"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	gstatus "google.golang.org/grpc/status"
)

func Assemble(stage repb.ExecutionStage_Value, name string, d *digest.InstanceNameDigest, er *repb.ExecuteResponse) (*longrunning.Operation, error) {
	if d == nil || er == nil {
		return nil, status.FailedPreconditionError("digest or execute response are both required to assemble operation")
	}
	metadata, err := ptypes.MarshalAny(&repb.ExecuteOperationMetadata{
		Stage:        stage,
		ActionDigest: d.Digest,
	})
	if err != nil {
		return nil, err
	}
	operation := &longrunning.Operation{
		Name:     name,
		Metadata: metadata,
	}
	result, err := ptypes.MarshalAny(er)
	if err != nil {
		return nil, err
	}
	operation.Result = &longrunning.Operation_Response{Response: result}

	if stage == repb.ExecutionStage_COMPLETED {
		operation.Done = true
	}
	return operation, nil
}

func AssembleFailed(stage repb.ExecutionStage_Value, name string, d *digest.InstanceNameDigest, status error) (*longrunning.Operation, error) {
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

func GetStateChangeFunc(stream StreamLike, taskID string, adInstanceDigest *digest.InstanceNameDigest) StateChangeFunc {
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

func GetFinishWithErrFunc(stream StreamLike, taskID string, adInstanceDigest *digest.InstanceNameDigest) FinishWithErrorFunc {
	return func(finalErr error) error {
		stage := repb.ExecutionStage_COMPLETED
		if op, err := AssembleFailed(stage, taskID, adInstanceDigest, finalErr); err == nil {
			log.Warningf("Failed action %q (returning err: %s via operation)", taskID, finalErr)

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
		}
		return finalErr
	}
}

func ExecuteResponseWithResult(ar *repb.ActionResult, summary *espb.ExecutionSummary, code codes.Code) *repb.ExecuteResponse {
	rsp := &repb.ExecuteResponse{
		Status: &statuspb.Status{Code: int32(code)},
	}
	if ar != nil {
		rsp.Result = ar
	}
	if summary != nil {
		if serialized, err := proto.Marshal(summary); err == nil {
			rsp.Message = base64.StdEncoding.EncodeToString(serialized)
		}
	}
	return rsp
}

func InProgressExecuteResponse() *repb.ExecuteResponse {
	return ExecuteResponseWithResult(nil, nil, codes.OK)
}

func ExtractStage(op *longrunning.Operation) repb.ExecutionStage_Value {
	md := &repb.ExecuteOperationMetadata{}
	if err := ptypes.UnmarshalAny(op.GetMetadata(), md); err != nil {
		return repb.ExecutionStage_UNKNOWN
	}
	return md.GetStage()
}

func ExtractExecuteResponse(op *longrunning.Operation) *repb.ExecuteResponse {
	er := &repb.ExecuteResponse{}
	if result := op.GetResult(); result != nil {
		if response, ok := result.(*longrunning.Operation_Response); ok {
			if err := ptypes.UnmarshalAny(response.Response, er); err == nil {
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
