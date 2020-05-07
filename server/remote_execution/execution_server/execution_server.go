package execution_server

import (
	"io"
	"log"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

type ExecutionServer struct {
	env      environment.Env
}

func NewExecutionServer(env environment.Env) (*ExecutionServer, error) {
	return &ExecutionServer{
		env:      env,
	}, nil
}

// Execute an action remotely.
//
// In order to execute an action, the client must first upload all of the
// inputs, the
// [Command][build.bazel.remote.execution.v2.Command] to run, and the
// [Action][build.bazel.remote.execution.v2.Action] into the
// [ContentAddressableStorage][build.bazel.remote.execution.v2.ContentAddressableStorage].
// It then calls `Execute` with an `action_digest` referring to them. The
// server will run the action and eventually return the result.
//
// The input `Action`'s fields MUST meet the various canonicalization
// requirements specified in the documentation for their types so that it has
// the same digest as other logically equivalent `Action`s. The server MAY
// enforce the requirements and return errors if a non-canonical input is
// received. It MAY also proceed without verifying some or all of the
// requirements, such as for performance reasons. If the server does not
// verify the requirement, then it will treat the `Action` as distinct from
// another logically equivalent action if they hash differently.
//
// Returns a stream of
// [google.longrunning.Operation][google.longrunning.Operation] messages
// describing the resulting execution, with eventual `response`
// [ExecuteResponse][build.bazel.remote.execution.v2.ExecuteResponse]. The
// `metadata` on the operation is of type
// [ExecuteOperationMetadata][build.bazel.remote.execution.v2.ExecuteOperationMetadata].
//
// If the client remains connected after the first response is returned after
// the server, then updates are streamed as if the client had called
// [WaitExecution][build.bazel.remote.execution.v2.Execution.WaitExecution]
// until the execution completes or the request reaches an error. The
// operation can also be queried using [Operations
// API][google.longrunning.Operations.GetOperation].
//
// The server NEED NOT implement other methods or functionality of the
// Operations API.
//
// Errors discovered during creation of the `Operation` will be reported
// as gRPC Status errors, while errors that occurred while running the
// action will be reported in the `status` field of the `ExecuteResponse`. The
// server MUST NOT set the `error` field of the `Operation` proto.
// The possible errors include:
//
// * `INVALID_ARGUMENT`: One or more arguments are invalid.
// * `FAILED_PRECONDITION`: One or more errors occurred in setting up the
//   action requested, such as a missing input or command or no worker being
//   available. The client may be able to fix the errors and retry.
// * `RESOURCE_EXHAUSTED`: There is insufficient quota of some resource to run
//   the action.
// * `UNAVAILABLE`: Due to a transient condition, such as all workers being
//   occupied (and the server does not support a queue), the action could not
//   be started. The client should retry.
// * `INTERNAL`: An internal error occurred in the execution engine or the
//   worker.
// * `DEADLINE_EXCEEDED`: The execution timed out.
// * `CANCELLED`: The operation was cancelled by the client. This status is
//   only possible if the server implements the Operations API CancelOperation
//   method, and it was called for the current execution.
//
// In the case of a missing input or command, the server SHOULD additionally
// send a [PreconditionFailure][google.rpc.PreconditionFailure] error detail
// where, for each requested blob not present in the CAS, there is a
// `Violation` with a `type` of `MISSING` and a `subject` of
// `"blobs/{hash}/{size}"` indicating the digest of the missing blob.
func (s *ExecutionServer) Execute(req *repb.ExecuteRequest, stream repb.Execution_ExecuteServer) error {
	exClient := s.env.GetExecutionClient()
	if exClient == nil {
		return status.FailedPreconditionError("No ExecutionClient was configured!")
	}

	workerStream, err := exClient.Execute(stream.Context(), req)
	if err != nil {
		return err
	}
	defer workerStream.CloseSend()
	for {
		op, readErr := workerStream.Recv()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return err
		}
		writeErr := stream.Send(op)
		if writeErr != nil {
			log.Printf("Execution client hung up -- ignoring")
		}
		// TODO(tylerw): Write operation to DB.
		log.Printf("BuildBuddy got operation back from worker: %+v", op)
	}
	return nil
}

// Wait for an execution operation to complete. When the client initially
// makes the request, the server immediately responds with the current status
// of the execution. The server will leave the request stream open until the
// operation completes, and then respond with the completed operation. The
// server MAY choose to stream additional updates as execution progresses,
// such as to provide an update as to the state of the execution.
func (s *ExecutionServer) WaitExecution(req *repb.WaitExecutionRequest, stream repb.Execution_WaitExecutionServer) error {
	log.Printf("BuildBuddy: WaitExecution called: %+v", req)
	// TODO(tylerw): Read operation from DB.
	return nil
}
