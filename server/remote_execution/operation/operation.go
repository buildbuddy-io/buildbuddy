package operation

import (
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/genproto/googleapis/longrunning"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	gstatus "google.golang.org/grpc/status"
)

func Assemble(stage repb.ExecutionStage_Value, d *digest.InstanceNameDigest, er *repb.ExecuteResponse) (string, *longrunning.Operation, error) {
	if d == nil || er == nil {
		return "", nil, status.FailedPreconditionError("digest or execute response are both required to assemble operation")
	}
	name := digest.DownloadResourceName(d.Digest, d.GetInstanceName())
	metadata, err := ptypes.MarshalAny(&repb.ExecuteOperationMetadata{
		Stage:        stage,
		ActionDigest: d.Digest,
	})
	if err != nil {
		return name, nil, err
	}
	operation := &longrunning.Operation{
		Name:     name,
		Metadata: metadata,
	}
	result, err := ptypes.MarshalAny(er)
	if err != nil {
		return name, nil, err
	}
	operation.Result = &longrunning.Operation_Response{Response: result}

	if stage == repb.ExecutionStage_COMPLETED {
		operation.Done = true
	}
	return name, operation, nil
}

func AssembleFailed(stage repb.ExecutionStage_Value, d *digest.InstanceNameDigest, status error) (*longrunning.Operation, error) {
	_, op, err := Assemble(stage, d, &repb.ExecuteResponse{
		Status: &statuspb.Status{
			Code:    int32(gstatus.Code(status)),
			Message: status.Error(),
		},
	})
	return op, err
}
