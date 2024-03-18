package execution_service

import (
	"context"
	"sort"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/execution"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"

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

func (es *ExecutionService) getInvocationExecutions(ctx context.Context, invocationID string) ([]*tables.Execution, error) {
	// Note: the invocation row may not be created yet because workflow
	// invocations are created by the execution itself.
	q := query_builder.NewQuery(`
		SELECT e.* FROM "InvocationExecutions" ie
		JOIN "Executions" e ON e.execution_id = ie.execution_id
		LEFT JOIN "Invocations" i ON i.invocation_id = e.invocation_id
	`)
	q.AddWhereClause(`ie.invocation_id = ?`, invocationID)
	dbh := es.env.GetDBHandle()

	permClauses, err := perms.GetPermissionsCheckClauses(ctx, es.env, q, "e")
	if err != nil {
		return nil, err
	}
	// If an authenticated invocation has OTHERS_READ perms (i.e. it is owned by a
	// group but made public), then let child executions inherit that OTHERS_READ
	// bit. An alternative here would be to explicitly mark all child executions
	// with OTHERS_READ, but that is somewhat complex. So we use this simple
	// permissions inheriting approach instead.
	permClauses.AddOr("i.perms IS NOT NULL AND i.perms & ? != 0", perms.OTHERS_READ)
	permQuery, permArgs := permClauses.Build()
	q.AddWhereClause("("+permQuery+")", permArgs...)

	queryStr, args := q.Build()
	rq := dbh.NewQuery(ctx, "execution_server_get_executions").Raw(queryStr, args...)
	return db.ScanAll(rq, &tables.Execution{})
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
		protoExec, err := execution.TableExecToClientProto(ex)
		if err != nil {
			return nil, err
		}
		rsp.Execution = append(rsp.Execution, protoExec)
	}
	if req.GetInlineExecuteResponse() {
		// If inlined responses are requested, fetch them now.
		var eg errgroup.Group
		for _, ex := range rsp.Execution {
			ex := ex
			eg.Go(func() error {
				res, err := execution.GetCachedExecuteResponse(ctx, es.env, ex.ExecutionId)
				if err != nil {
					return err
				}
				ex.ExecuteResponse = res
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			log.CtxInfof(ctx, "Failed to fetch inline execution response(s): %s", err)
		}
	}
	return rsp, nil
}
