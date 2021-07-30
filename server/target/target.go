package target

import (
	"context"
	"fmt"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"
	"github.com/golang/protobuf/ptypes"

	cmpb "github.com/buildbuddy-io/buildbuddy/proto/api/v1/common"
	trpb "github.com/buildbuddy-io/buildbuddy/proto/target"
)

const (
	sqlite3Dialect = "sqlite3"
)

func convertToCommonStatus(in build_event_stream.TestStatus) cmpb.Status {
	switch in {
	case build_event_stream.TestStatus_NO_STATUS:
		return cmpb.Status_STATUS_UNSPECIFIED
	case build_event_stream.TestStatus_PASSED:
		return cmpb.Status_PASSED
	case build_event_stream.TestStatus_FLAKY:
		return cmpb.Status_FLAKY
	case build_event_stream.TestStatus_TIMEOUT:
		return cmpb.Status_TIMED_OUT
	case build_event_stream.TestStatus_FAILED:
		return cmpb.Status_FAILED
	case build_event_stream.TestStatus_INCOMPLETE:
		return cmpb.Status_INCOMPLETE
	case build_event_stream.TestStatus_REMOTE_FAILURE:
		return cmpb.Status_TOOL_FAILED
	case build_event_stream.TestStatus_FAILED_TO_BUILD:
		return cmpb.Status_FAILED_TO_BUILD
	case build_event_stream.TestStatus_TOOL_HALTED_BEFORE_TESTING:
		return cmpb.Status_INCOMPLETE
	default:
		return cmpb.Status_STATUS_UNSPECIFIED
	}
}

func GetTarget(ctx context.Context, env environment.Env, req *trpb.GetTargetRequest) (*trpb.GetTargetResponse, error) {
	auth := env.GetAuthenticator()
	if auth == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	_, err := auth.AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	startUsec := int64(0)
	endUsec := timeutil.ToUsec(time.Now())
	if st := req.GetStartTimeUsec(); st != 0 {
		startUsec = st
	}
	if et := req.GetEndTimeUsec(); et != 0 {
		endUsec = et
	}
	targetHistory, err := readTargets(ctx, env, req, startUsec, endUsec)
	if err != nil {
		return nil, err
	}
	return &trpb.GetTargetResponse{
		InvocationTargets: targetHistory,
	}, nil
}

func readTargets(ctx context.Context, env environment.Env, req *trpb.GetTargetRequest, startUsec, endUsec int64) ([]*trpb.TargetHistory, error) {
	if env.GetDBHandle() == nil {
		return nil, status.FailedPreconditionError("database not configured")
	}
	q := query_builder.NewQuery(`SELECT t.target_id, t.label, t.rule_type, ts.target_type,
                                     ts.test_size, ts.status, ts.start_time_usec, ts.duration_usec,
                                     i.invocation_id, i.commit_sha, i.branch_name, i.repo_url, i.created_at_usec
                                     FROM Targets as t
                                     JOIN TargetStatuses AS ts ON t.target_id = ts.target_id
                                     JOIN Invocations AS i ON ts.invocation_pk = i.invocation_pk`)
	q.AddWhereClause("i.group_id = ?", req.GetRequestContext().GetGroupId())
	q.AddWhereClause("t.group_id = ?", req.GetRequestContext().GetGroupId())
	// Adds user / permissions to targets (t) table.
	if err := perms.AddPermissionsCheckToQueryWithTableAlias(ctx, env, q, "t"); err != nil {
		return nil, err
	}
	// Adds user / permissions to invocations (i) table.
	if err := perms.AddPermissionsCheckToQueryWithTableAlias(ctx, env, q, "i"); err != nil {
		return nil, err
	}
	q.AddWhereClause("i.created_at_usec > ?", startUsec)
	q.AddWhereClause("i.created_at_usec + i.duration_usec < ?", endUsec)

	tq := req.GetQuery()
	if repo := tq.GetRepoUrl(); repo != "" {
		q.AddWhereClause("i.repo_url = ?", repo)
	}
	if user := tq.GetUser(); user != "" {
		q.AddWhereClause("i.user = ?", user)
	}
	if host := tq.GetHost(); host != "" {
		q.AddWhereClause("i.host = ?", host)
	}
	if sha := tq.GetCommitSha(); sha != "" {
		q.AddWhereClause("i.commit_sha = ?", sha)
	}
	if role := tq.GetRole(); role != "" {
		q.AddWhereClause("i.role = ?", role)
	}
	if targetType := tq.GetTargetType(); targetType != cmpb.TargetType_TARGET_TYPE_UNSPECIFIED {
		q.AddWhereClause("ts.target_type = ?", int32(targetType))
	}
	if branchName := tq.GetBranchName(); branchName != "" {
		q.AddWhereClause("i.branch_name = ?", branchName)
	}
	q.SetOrderBy("t.label ASC, i.created_at_usec", false /*=ascending*/)
	queryStr, args := q.Build()

	seenTargets := make(map[string]struct{}, 0)
	targets := make([]*trpb.Target, 0)
	statuses := make(map[string][]*trpb.TargetStatus, 0)

	err := env.GetDBHandle().Transaction(ctx, func(tx *db.DB) error {
		rows, err := tx.Raw(queryStr, args...).Rows()
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			row := struct {
				Label         string
				RuleType      string
				CommitSHA     string
				BranchName    string
				RepoURL       string
				InvocationID  string
				TargetID      int64
				CreatedAtUsec int64
				StartTimeUsec int64
				DurationUsec  int64
				TargetType    int32
				TestSize      int32
				Status        int32
			}{}
			if err := tx.ScanRows(rows, &row); err != nil {
				return err
			}
			targetID := fmt.Sprintf("%d", row.TargetID)
			if _, ok := seenTargets[targetID]; !ok {
				seenTargets[targetID] = struct{}{}
				targets = append(targets, &trpb.Target{
					Id:         targetID,
					Label:      row.Label,
					RuleType:   row.RuleType,
					TargetType: cmpb.TargetType(row.TargetType),
					TestSize:   cmpb.TestSize(row.TestSize),
				})
			}

			tsPb, _ := ptypes.TimestampProto(timeutil.FromUsec(row.StartTimeUsec))
			statuses[targetID] = append(statuses[targetID], &trpb.TargetStatus{
				InvocationId: row.InvocationID,
				CommitSha:    row.CommitSHA,
				Status:       convertToCommonStatus(build_event_stream.TestStatus(row.Status)),
				Timing: &cmpb.Timing{
					StartTime: tsPb,
					Duration:  ptypes.DurationProto(time.Microsecond * time.Duration(row.DurationUsec)),
				},
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	targetHistories := make([]*trpb.TargetHistory, 0, len(targets))
	for _, target := range targets {
		targetID := target.Id
		targetHistories = append(targetHistories, &trpb.TargetHistory{
			Target:       target,
			TargetStatus: statuses[targetID],
			RepoUrl:      tq.GetRepoUrl(),
		})
	}
	return targetHistories, nil
}
