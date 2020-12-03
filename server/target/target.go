package target

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/ptypes"
	"github.com/jinzhu/gorm"

	cmpb "github.com/buildbuddy-io/buildbuddy/proto/api/v1/common"
	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	trpb "github.com/buildbuddy-io/buildbuddy/proto/target"
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
	endUsec := int64(time.Now().UnixNano() / 1000)
	if st := req.GetStartTimeUsec(); st != 0 {
		startUsec = startUsec
	}
	if et := req.GetEndTimeUsec(); et != 0 {
		endUsec = et
	}
	targetHistory, err := readTargetHistory(ctx, env, req.GetQuery(), startUsec, endUsec)
	if err != nil {
		return nil, err
	}
	return &trpb.GetTargetResponse{
		InvocationTargets: targetHistory,
	}, nil
}

func readTargets(ctx context.Context, env environment.Env, tq *trpb.TargetQuery) ([]*tables.Target, error) {
	if env.GetDBHandle() == nil {
		return nil, status.FailedPreconditionError("database not configured")
	}
	q := query_builder.NewQuery(`SELECT * FROM Targets as t`)
	q.AddWhereClause("t.repo_url = ?", tq.GetRepoUrl())
	if err := perms.AddPermissionsCheckToQueryWithTableAlias(ctx, env, q, "t"); err != nil {
		return nil, err
	}
	queryStr, args := q.Build()
	targets := make([]*tables.Target, 0)
	err := env.GetDBHandle().Transaction(func(tx *gorm.DB) error {
		rows, err := tx.Raw(queryStr, args...).Rows()
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			t := tables.Target{}
			if err := tx.ScanRows(rows, &t); err != nil {
				return err
			}
			targets = append(targets, &t)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return targets, nil
}

func readTargetHistory(ctx context.Context, env environment.Env, tq *trpb.TargetQuery, startUsec, endUsec int64) ([]*trpb.TargetHistory, error) {
	if env.GetDBHandle() == nil {
		return nil, status.FailedPreconditionError("database not configured")
	}
	allTargets, err := readTargets(ctx, env, tq)
	if err != nil {
		return nil, err
	}
	targets := make(map[int64]*trpb.Target, 0)
	for _, t := range allTargets {
		targets[t.TargetID] = &trpb.Target{
			Id:       fmt.Sprintf("%d", t.TargetID),
			Label:    t.Label,
			RuleType: t.RuleType,
		}
	}

	q := query_builder.NewQuery(`SELECT ts.target_id, ts.target_type, ts.test_size, ts.status, ts.start_time_usec,
                                     ts.duration_usec, i.invocation_id, i.commit_sha, i.repo_url, i.created_at_usec
                                     FROM TargetStatuses AS ts
                                     JOIN Invocations AS i ON ts.invocation_pk == i.invocation_pk`)
	// Adds user / permissions to invocations (i) table.
	if err := perms.AddPermissionsCheckToQueryWithTableAlias(ctx, env, q, "i"); err != nil {
		return nil, err
	}
	q.AddWhereClause("i.created_at_usec > ?", startUsec)
	q.AddWhereClause("i.created_at_usec + i.duration_usec < ?", endUsec)
	q.AddWhereClause("i.repo_url = ?", tq.GetRepoUrl())
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
	queryStr, args := q.Build()
	statuses := make(map[int64][]*trpb.TargetStatus, 0)

	err = env.GetDBHandle().Transaction(func(tx *gorm.DB) error {
		rows, err := tx.Raw(queryStr, args...).Rows()
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			row := struct {
				TargetID      int64
				TargetType    int32
				TestSize      int32
				Status        int32
				StartTimeUsec int64
				DurationUsec  int64
				InvocationID  string
				CommitSHA     string
				RepoURL       string
				CreatedAtUsec int64
			}{}
			if err := tx.ScanRows(rows, &row); err != nil {
				return err
			}
			if _, ok := targets[row.TargetID]; ok {
				targets[row.TargetID].TargetType = cmpb.TargetType(row.TargetType)
				targets[row.TargetID].TestSize = cmpb.TestSize(row.TestSize)
			}

			tsPb, _ := ptypes.TimestampProto(time.Unix(0, row.StartTimeUsec*int64(time.Microsecond)))
			statuses[row.TargetID] = append(statuses[row.TargetID], &trpb.TargetStatus{
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
	for targetID, target := range targets {
		sort.Slice(statuses[targetID], func(i, j int) bool {
			iStart := statuses[targetID][i].GetTiming().GetStartTime().GetSeconds()
			jStart := statuses[targetID][j].GetTiming().GetStartTime().GetSeconds()
			return iStart < jStart
		})
		targetHistories = append(targetHistories, &trpb.TargetHistory{
			Target:       target,
			TargetStatus: statuses[targetID],
			RepoUrl:      tq.GetRepoUrl(),
		})
	}
	return targetHistories, nil
}
