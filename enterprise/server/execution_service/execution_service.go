package execution_service

import (
	"context"
	"sort"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/execution"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
)

type ExecutionService struct {
	env environment.Env
}

func NewExecutionService(env environment.Env) *ExecutionService {
	return &ExecutionService{
		env: env,
	}
}

func checkPreconditions(req *espb.GetExecutionRequest) error {
	if req.GetExecutionLookup().GetInvocationId() != "" {
		return nil
	}
	return status.FailedPreconditionError("An execution lookup with invocation_id must be provided")
}

func (es *ExecutionService) addExecutionPermissionsChecks(ctx context.Context, q *query_builder.Query) error {
	permClauses, err := perms.GetPermissionsCheckClauses(ctx, es.env, q, "e")
	if err != nil {
		return err
	}
	// If an authenticated invocation has OTHERS_READ perms (i.e. it is owned by a
	// group but made public), then let child executions inherit that OTHERS_READ
	// bit. An alternative here would be to explicitly mark all child executions
	// with OTHERS_READ, but that is somewhat complex. So we use this simple
	// permissions inheriting approach instead.
	permClauses.AddOr("i.perms & ? != 0", perms.OTHERS_READ)
	permQuery, permArgs := permClauses.Build()
	q.AddWhereClause("("+permQuery+")", permArgs...)
	return nil
}

func (es *ExecutionService) queryExecutions(ctx context.Context, baseQuery *query_builder.Query) ([]tables.Execution, error) {
	dbh := es.env.GetDBHandle()
	q := baseQuery

	if err := es.addExecutionPermissionsChecks(ctx, q); err != nil {
		return nil, err
	}

	queryStr, args := q.Build()
	rows, err := dbh.DB(ctx).Raw(queryStr, args...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	executions := make([]tables.Execution, 0)
	for rows.Next() {
		var exec tables.Execution
		if err := dbh.DB(ctx).ScanRows(rows, &exec); err != nil {
			return nil, err
		}
		executions = append(executions, exec)
	}
	return executions, nil
}

func (es *ExecutionService) getInvocationExecutions(ctx context.Context, invocationID string) ([]tables.Execution, error) {
	q := query_builder.NewQuery(`
		SELECT e.* FROM "InvocationExecutions" ie
		JOIN "Executions" e ON e.execution_id = ie.execution_id
		JOIN "Invocations" i ON i.invocation_id = e.invocation_id
	`)
	q.AddWhereClause(`ie.invocation_id = ?`, invocationID)
	return es.queryExecutions(ctx, q)
}

func (es *ExecutionService) GetExecution(ctx context.Context, req *espb.GetExecutionRequest) (*espb.GetExecutionResponse, error) {
	if es.env.GetDBHandle() == nil {
		return nil, status.FailedPreconditionError("database not configured")
	}
	if err := checkPreconditions(req); err != nil {
		return nil, err
	}
	executions, err := es.getInvocationExecutions(ctx, req.GetExecutionLookup().GetInvocationId())
	if err != nil {
		return nil, err
	}
	// Sort the executions by start time.
	sort.Slice(executions, func(i, j int) bool {
		return executions[i].Model.CreatedAtUsec < executions[j].Model.CreatedAtUsec
	})
	rsp := &espb.GetExecutionResponse{}
	for _, ex := range executions {
		protoExec, err := execution.TableExecToClientProto(&ex)
		if err != nil {
			return nil, err
		}
		rsp.Execution = append(rsp.Execution, protoExec)
	}
	return rsp, nil
}

func (es *ExecutionService) GetExecutionInputTree(ctx context.Context, req *espb.GetExecutionInputTreeRequest) (*espb.GetExecutionResponse, error) {
	if es.env.GetDBHandle() == nil {
		return nil, status.FailedPreconditionError("database not configured")
	}
	if err := checkPreconditions(req); err != nil {
		return nil, err
	}
	// Fetch the one execution that we're looking for.  Check user access as
	// part of this lookup.
	q := query_builder.NewQuery(`
		SELECT e.* FROM "Executions" e
		JOIN "Invocations" i ON i.invocation_id = e.invocation_id
	`)
	q.AddWhereClause(`e.execution_id = ?`, req.GetExecutionId())
	q.AddWhereClause(`e.invocation_id = ?`, req.GetInvocationId())
	if err := es.addExecutionPermissionsChecks(ctx, q); err != nil {
		return nil, err
	}
	queryStr, args := q.Build()
	rows, err := dbh.DB(ctx).Raw(queryStr, args...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	executions := make([]tables.Execution, 0)
	for rows.Next() {
		var exec tables.Execution
		if err := dbh.DB(ctx).ScanRows(rows, &exec); err != nil {
			return nil, err
		}
		executions = append(executions, exec)
	}
	if len(executions) > 1 {
		return nil, status.InternalError("Found two executions with same ID.")
	}
	if len(executions) < 1 {
		return nil, status.NotFoundError("Execution not found.")
	}
	e, err := execution.TableExecToClientProto(&executions[0])

	// Bail if this action just looks too big.
	// XXX: Flag.
	if e.FileDownloadCount > 5000 {
		// XXX: Valid but empty response?
		return nil, status.FailedPreconditionError("Too large.")
	}

	// Fetch the action digest.
	actionDigest, err := digest.ParseDownloadResourceName(e.ExecutionID)
	if err != nil {
		return nil, err
	}

	actionDigest

	// Get the input root digest from the action.

	// Fetch the full tree.
	//cachetools.GetTreeFromRootDirectoryDigest(e.)

	executions, err := es.getInvocationExecutions(ctx, req.GetExecutionLookup().GetInvocationId())
	if err != nil {
		return nil, err
	}
	return nil, nil
}
