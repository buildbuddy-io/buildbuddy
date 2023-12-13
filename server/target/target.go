package target

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/event_index"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/paging"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	cmpb "github.com/buildbuddy-io/buildbuddy/proto/api/v1/common"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	ispb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	pgpb "github.com/buildbuddy-io/buildbuddy/proto/pagination"
	trpb "github.com/buildbuddy-io/buildbuddy/proto/target"
	tppb "github.com/buildbuddy-io/buildbuddy/proto/target_pagination"
	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
)

var (
	readTargetStatusesFromOLAPDBEnabled = flag.Bool("app.enable_read_target_statuses_from_olap_db", false, "If enabled, read target statuses from OLAP DB")
)

const (
	sqlite3Dialect  = "sqlite3"
	ciRole          = "CI"
	testCommand     = "test"
	coverageCommand = "coverage"

	// The number of distinct commits returned in GetTargetHistoryResponse.
	targetHistoryPageSize = 20

	// The max number of targets returned in each TargetGroup page.
	// TODO(bduffany): let the client set this. We want this to be 100 when on
	// the Targets tab but 10 when on the overview tab.
	targetPageSize = 12

	// When returning a paginated list of all targets in an invocation with
	// files expanded, stop returning targets after this many files have been
	// expanded. This is a "soft" limit because the file list is not truncated,
	// only the target list. So for example if a target contains 101 files, all
	// 101 files will be returned, but only that target will be returned in the
	// target page.
	targetFilesSoftLimit = 100
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
		return cmpb.Status_CANCELLED
	default:
		return cmpb.Status_STATUS_UNSPECIFIED
	}
}

func NewTokenFromRequest(req *trpb.GetTargetHistoryRequest) (*tppb.PaginationToken, error) {
	strToken := req.GetPageToken()
	if strToken == "" {
		return nil, nil
	}

	data, err := base64.StdEncoding.DecodeString(strToken)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("failed to decode page token %q: %s", strToken, err)
	}
	t := &tppb.PaginationToken{}
	if err := proto.Unmarshal(data, t); err != nil {
		return nil, status.InvalidArgumentErrorf("failed to unmarshal page token: %s", err)
	}
	return t, nil
}

func EncodePaginationToken(token *tppb.PaginationToken) (string, error) {
	data, err := proto.Marshal(token)
	if err != nil {
		return "", status.InvalidArgumentErrorf("failed to marshal page token: %s", err)
	}
	str := base64.StdEncoding.EncodeToString(data)
	return str, nil
}

func ApplyToQuery(timestampField string, t *tppb.PaginationToken, q *query_builder.Query) {
	o := query_builder.OrClauses{}
	o.AddOr(fmt.Sprintf("%s < ? ", timestampField), t.GetInvocationEndTimeUsec())
	o.AddOr(fmt.Sprintf("(%s = ? AND commit_sha > ?)", timestampField), t.GetInvocationEndTimeUsec(), t.GetCommitSha())
	orQuery, orArgs := o.Build()
	q.AddWhereClause("("+orQuery+")", orArgs...)
}

func GetTargetHistory(ctx context.Context, env environment.Env, req *trpb.GetTargetHistoryRequest) (*trpb.GetTargetHistoryResponse, error) {
	auth := env.GetAuthenticator()
	if auth == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	_, err := auth.AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}

	if isReadFromOLAPDBEnabled(env) {
		return readPaginatedTargetsFromOLAPDB(ctx, env, req)
	}
	return readPaginatedTargetsFromPrimaryDB(ctx, env, req)
}

func GetTarget(ctx context.Context, env environment.Env, inv *inpb.Invocation, idx *event_index.Index, req *trpb.GetTargetRequest) (*trpb.GetTargetResponse, error) {
	page, err := paging.DecodeOffsetLimit(req.GetPageToken())
	if err != nil {
		return nil, err
	}
	if req.GetTargetLabel() != "" && req.GetStatus() == 0 {
		// When fetching a single target label, the status field is optional (to
		// support old invocation links that didn't require the 'targetStatus'
		// param). In this case, use the test status if available (which is
		// usually more interesting); otherwise use the build status.
		target := idx.TestTargetByLabel[req.GetTargetLabel()]
		if target == nil {
			target = idx.BuildTargetByLabel[req.GetTargetLabel()]
		}
		if target == nil {
			return nil, status.NotFoundErrorf("target %q not found for invocation %s", req.GetTargetLabel(), inv.GetInvocationId())
		}
		req = proto.Clone(req).(*trpb.GetTargetRequest)
		req.Status = &target.Status
	}

	var statuses []cmpb.Status
	// Note: req.Status == nil means the status was unset. req.Status will be
	// non-nil and set to 0 when explicitly requesting the artifact listing.
	if req.GetTargetLabel() == "" && req.Status == nil {
		// Requesting a general target listing; fetch initial data pages for
		// status 0 (for the top-level target+files listing), plus each status
		// appearing in the build (just metadata).
		statuses = append(statuses, 0)
		for s := range idx.TargetsByStatus {
			statuses = append(statuses, s)
		}
	} else {
		// Requesting a specific target status.
		statuses = []cmpb.Status{req.GetStatus()}
	}

	// Build up the TargetGroup for each status.
	res := &trpb.GetTargetResponse{
		TargetGroups: make([]*trpb.TargetGroup, 0, len(statuses)),
	}
	for _, s := range statuses {
		g := &trpb.TargetGroup{Status: s}
		res.TargetGroups = append(res.TargetGroups, g)

		var labels []string
		if req.GetTargetLabel() != "" {
			labels = append(labels, req.GetTargetLabel())
		} else if s == 0 {
			labels = idx.AllTargetLabels
		} else {
			targets := idx.TargetsByStatus[s]
			for _, t := range targets {
				labels = append(labels, t.GetMetadata().GetLabel())
			}
		}
		if s == 0 {
			labels = labelsWithFilesAndMatchingFilter(idx, labels, req.GetFilter())
		} else {
			labels = labelsMatchingFilter(labels, req.GetFilter())
		}

		// Set TotalCount based on the length of the label list *before* slicing
		// based on the page token.
		g.TotalCount = int64(len(labels))

		// Note, using > and not >= here since the offset is allowed to be
		// exactly equal to the length, meaning that the invocation is still in
		// progress and the client should try to fetch the next page starting
		// with the last available offset.
		if page.Offset > int64(len(labels)) {
			return nil, status.InvalidArgumentErrorf("invalid page offset (offset %d, max %d)", page.Offset, len(labels))
		}
		labels = labels[page.Offset:]
		if page.Limit == 0 || page.Limit > targetPageSize {
			page.Limit = targetPageSize
		}
		if int64(len(labels)) > page.Limit {
			labels = labels[:page.Limit]
		}

		totalFileCount := 0
		nextOffset := page.Offset
		for i, label := range labels {
			var target *trpb.Target
			isTestStatus := false
			switch s {
			case 0:
				target = &trpb.Target{Metadata: &trpb.TargetMetadata{Label: label}}
			case cmpb.Status_BUILDING, cmpb.Status_BUILT, cmpb.Status_FAILED_TO_BUILD, cmpb.Status_SKIPPED:
				target = idx.BuildTargetByLabel[label]
			default:
				target = idx.TestTargetByLabel[label]
				isTestStatus = true
			}
			if target == nil {
				return nil, status.InternalErrorf("missing required events for target label = %q, status = %d", label, s)
			}
			// Clone to avoid messing with the indexed target.
			target = proto.Clone(target).(*trpb.Target)

			// Expand files only if requesting a single target label in the
			// request, or when fetching the TargetGroup with status unset (i.e.
			// the "general" target listing used for the Artifacts card).
			if s == 0 || req.GetTargetLabel() != "" {
				target.Files = filesForLabel(idx, label, req.GetFilter())
				totalFileCount += len(target.Files)
			}
			// Expand TestResult events only when fetching a single label and
			// if requesting the test status.
			if req.GetTargetLabel() != "" && isTestStatus {
				target.TestResultEvents = idx.TestResultEventsByLabel[label]
			}
			// When fetching a single label, expand Action events matching
			// whichever target configuration we happened to store in the
			// Completed map.
			//
			// TODO: include both label and configuration ID to deal with
			// transitions.
			if req.GetTargetLabel() != "" {
				target.ActionEvents = actionEventsForLabel(idx, label)
			}

			g.Targets = append(g.Targets, target)
			nextOffset = page.Offset + int64(i) + 1

			if totalFileCount > targetFilesSoftLimit {
				break
			}
		}
		// When fetching multiple targets (and not just a single target), set
		// the next page token if there are more targets to fetch or if the
		// invocation is in progress (since there may be more targets available
		// on the next fetch).
		if req.GetTargetLabel() == "" && (nextOffset < g.TotalCount || inv.InvocationStatus == ispb.InvocationStatus_PARTIAL_INVOCATION_STATUS) {
			tok, err := paging.EncodeOffsetLimit(&pgpb.OffsetLimit{
				Offset: nextOffset,
				Limit:  page.Limit,
			})
			if err != nil {
				return nil, err
			}
			g.NextPageToken = tok
		}

		// When the invocation is in progress, we usually return a non-empty
		// page token so that the client knows there may be more results since
		// the last fetch. However, if the page token offset is within the first
		// page, then the fetched results will overlap with the initial page of
		// results that is fetched as part of the full GetInvocation refresh
		// that the UI does every 3s. The client has to somehow deal with this
		// overlap, which adds some complexity. So for now, we just return an
		// empty page token whenever there is less than a single page of data
		// available.
		if g.TotalCount < page.Limit {
			g.NextPageToken = ""
		}
	}
	return res, nil
}

// ActionCompletedId represented as a go struct so it can be used as a map
// key.
type actionKey struct{ label, primaryOutput, configurationID string }

func actionKeyFromProto(protoID *build_event_stream.BuildEventId_ActionCompletedId) actionKey {
	return actionKey{
		label:           protoID.GetLabel(),
		primaryOutput:   protoID.GetPrimaryOutput(),
		configurationID: protoID.GetConfiguration().GetId(),
	}
}

func actionEventsForLabel(idx *event_index.Index, label string) []*build_event_stream.BuildEvent {
	// The Completed event will declare child Action events that it expects to
	// see later in the stream. Collect these Action IDs and then filter
	// idx.ActionEvents to just the ones matching these IDs.
	completed := idx.TargetCompleteEventByLabel[label]
	targetActionKeys := map[actionKey]struct{}{}
	for _, c := range completed.GetChildren() {
		if protoID := c.GetActionCompleted(); protoID != nil {
			targetActionKeys[actionKeyFromProto(protoID)] = struct{}{}
		}
	}
	var out []*build_event_stream.BuildEvent
	for _, event := range idx.ActionEvents {
		key := actionKeyFromProto(event.GetId().GetActionCompleted())
		if _, ok := targetActionKeys[key]; ok {
			out = append(out, event)
		}
	}
	return out
}

// Returns the labels that have at least one file associated with them, AND
// either the target label or some file path matches the given filter.
func labelsWithFilesAndMatchingFilter(idx *event_index.Index, labels []string, filter string) []string {
	out := make([]string, 0, len(labels))
	for _, label := range labels {
		if hasFilesAndMatchesFilter(idx, label, filter) {
			out = append(out, label)
		}
	}
	return out
}

func labelsMatchingFilter(labels []string, filter string) []string {
	if filter == "" {
		return labels
	}
	var out []string
	filterLower := strings.ToLower(filter)
	for _, label := range labels {
		if strings.Contains(strings.ToLower(label), filterLower) {
			out = append(out, label)
		}
	}
	return out
}

// Returns whether either (a) the label has any files whose paths match the
// given filter, or (b) the label itself matches the filter and it has at least
// one file.
func hasFilesAndMatchesFilter(idx *event_index.Index, label, filter string) bool {
	completedEvent := idx.TargetCompleteEventByLabel[label]
	if completedEvent == nil {
		return false
	}
	filterLower := strings.ToLower(filter)
	filterMatchesLabel := strings.Contains(strings.ToLower(label), filterLower)
	for _, g := range completedEvent.GetCompleted().GetOutputGroup() {
		for _, s := range g.GetFileSets() {
			for _, f := range idx.NamedSetOfFilesByID[s.GetId()].GetFiles() {
				if f.GetUri() == "" {
					// Ignore inlined file contents and symlinks for now.
					// TODO: render these in the UI so that we can just
					// return `len(OutputGroup) > 0` here.
					continue
				}
				if filterMatchesLabel || strings.Contains(strings.ToLower(filePath(f)), filterLower) {
					return true
				}
			}
		}
	}
	return false
}

func filesForLabel(idx *event_index.Index, label, filter string) []*build_event_stream.File {
	completedEvent := idx.TargetCompleteEventByLabel[label]
	if completedEvent == nil {
		return nil
	}
	var out []*build_event_stream.File
	var matched []*build_event_stream.File
	filterLower := strings.ToLower(filter)
	fullPath := map[*build_event_stream.File]string{}
	for _, g := range completedEvent.GetCompleted().GetOutputGroup() {
		for _, s := range g.GetFileSets() {
			for _, f := range idx.NamedSetOfFilesByID[s.GetId()].GetFiles() {
				if f.GetUri() == "" {
					// Ignore inlined file contents and symlinks for now.
					continue
				}
				fullPath[f] = filePath(f)
				out = append(out, f)
				if strings.Contains(strings.ToLower(fullPath[f]), filterLower) {
					matched = append(matched, f)
				}
			}
		}
	}
	// If no files matched, then it must have been that only the target label
	// was matched, so just return all files for the target label. Otherwise,
	// return just the matching files.
	if len(matched) > 0 {
		out = matched
	}
	sort.Slice(out, func(i, j int) bool {
		return fullPath[out[i]] < fullPath[out[j]]
	})
	return out
}

func filePath(f *build_event_stream.File) string {
	path := append([]string{}, f.GetPathPrefix()...)
	path = append(path, f.GetName())
	return strings.Join(path, "/")
}

func fetchTargetsFromOLAPDB(ctx context.Context, env environment.Env, q *query_builder.Query, repoURL string, groupID string) (*trpb.GetTargetHistoryResponse, error) {
	qStr, qArgs := q.Build()

	seenTargets := make(map[string]struct{}, 0)
	targets := make([]*trpb.TargetMetadata, 0)
	statuses := make(map[string][]*trpb.TargetStatus, 0)
	var nextPageToken *tppb.PaginationToken

	db := env.GetOLAPDBHandle().DB(ctx)

	rows, err := db.Raw(qStr, qArgs...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		row := struct {
			Label                   string
			RuleType                string
			TargetType              int32
			TestSize                int32
			Status                  int32
			StartTimeUsec           int64
			DurationUsec            int64
			InvocationUUID          string
			CommitSHA               string
			BranchName              string
			RepoURL                 string
			InvocationStartTimeUsec int64
		}{}
		if err := db.ScanRows(rows, &row); err != nil {
			return nil, err
		}
		if _, ok := seenTargets[row.Label]; !ok {
			seenTargets[row.Label] = struct{}{}
			targets = append(targets, &trpb.TargetMetadata{
				Label:      row.Label,
				RuleType:   row.RuleType,
				TargetType: cmpb.TargetType(row.TargetType),
				TestSize:   cmpb.TestSize(row.TestSize),
			})
		}

		invocationID, err := uuid.Base64StringToString(row.InvocationUUID)
		if err != nil {
			log.Errorf("cannot parse invocation_id for row (group_id, %q, repo_url: %q, label: %q, invocation_uuid: %q", groupID, repoURL, row.Label, row.InvocationUUID)
			continue
		}

		statuses[row.Label] = append(statuses[row.Label], &trpb.TargetStatus{
			InvocationId: invocationID,
			CommitSha:    row.CommitSHA,
			Status:       convertToCommonStatus(build_event_stream.TestStatus(row.Status)),
			Timing: &cmpb.Timing{
				StartTime: timestamppb.New(time.UnixMicro(row.StartTimeUsec)),
				Duration:  durationpb.New(time.Microsecond * time.Duration(row.DurationUsec)),
			},
			InvocationCreatedAtUsec: row.InvocationStartTimeUsec,
		})
		if nextPageToken == nil || nextPageToken.GetInvocationEndTimeUsec() >= row.InvocationStartTimeUsec {
			nextPageToken = &tppb.PaginationToken{
				InvocationEndTimeUsec: row.InvocationStartTimeUsec,
			}
			if nextPageToken.GetInvocationEndTimeUsec() == row.InvocationStartTimeUsec && nextPageToken.GetCommitSha() > row.CommitSHA {
				continue
			}
			nextPageToken.CommitSha = row.CommitSHA
		}
	}
	if err != nil {
		return nil, err
	}

	targetHistories := make([]*trpb.TargetHistory, 0, len(targets))
	for _, target := range targets {
		targetHistories = append(targetHistories, &trpb.TargetHistory{
			Target:       target,
			TargetStatus: statuses[target.GetLabel()],
			RepoUrl:      repoURL,
		})
	}
	resp := &trpb.GetTargetHistoryResponse{
		InvocationTargets: targetHistories,
	}
	if nextPageToken != nil {
		tokenStr, err := EncodePaginationToken(nextPageToken)
		if err != nil {
			return nil, err
		}
		resp.NextPageToken = tokenStr
	}
	return resp, nil
}

func fetchTargetsFromPrimaryDB(ctx context.Context, env environment.Env, q *query_builder.Query, repoURL string) (*trpb.GetTargetHistoryResponse, error) {
	queryStr, args := q.Build()

	seenTargets := make(map[string]struct{}, 0)
	targets := make([]*trpb.TargetMetadata, 0)
	statuses := make(map[string][]*trpb.TargetStatus, 0)
	var nextPageToken *tppb.PaginationToken

	type target struct {
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
	}

	err := env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		rq := tx.NewQuery(ctx, "target_get_target_history").Raw(queryStr, args...)
		return db.ScanEach(rq, func(ctx context.Context, row *target) error {
			targetID := fmt.Sprintf("%d", row.TargetID)
			if _, ok := seenTargets[targetID]; !ok {
				seenTargets[targetID] = struct{}{}
				targets = append(targets, &trpb.TargetMetadata{
					Id:         targetID,
					Label:      row.Label,
					RuleType:   row.RuleType,
					TargetType: cmpb.TargetType(row.TargetType),
					TestSize:   cmpb.TestSize(row.TestSize),
				})
			}

			statuses[targetID] = append(statuses[targetID], &trpb.TargetStatus{
				InvocationId: row.InvocationID,
				CommitSha:    row.CommitSHA,
				Status:       convertToCommonStatus(build_event_stream.TestStatus(row.Status)),
				Timing: &cmpb.Timing{
					StartTime: timestamppb.New(time.UnixMicro(row.StartTimeUsec)),
					Duration:  durationpb.New(time.Microsecond * time.Duration(row.DurationUsec)),
				},
				InvocationCreatedAtUsec: row.CreatedAtUsec,
			})
			if nextPageToken == nil || nextPageToken.GetInvocationEndTimeUsec() >= row.CreatedAtUsec {
				nextPageToken = &tppb.PaginationToken{
					InvocationEndTimeUsec: row.CreatedAtUsec,
				}
				if nextPageToken.GetInvocationEndTimeUsec() == row.CreatedAtUsec && nextPageToken.GetCommitSha() > row.CommitSHA {
					return nil
				}
				nextPageToken.CommitSha = row.CommitSHA
			}
			return nil
		})
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
			RepoUrl:      repoURL,
		})
	}
	resp := &trpb.GetTargetHistoryResponse{
		InvocationTargets: targetHistories,
	}
	if nextPageToken != nil {
		tokenStr, err := EncodePaginationToken(nextPageToken)
		if err != nil {
			return nil, err
		}
		resp.NextPageToken = tokenStr
	}
	return resp, nil
}

func getRepoURL(req *trpb.GetTargetHistoryRequest) (string, error) {
	repo := req.GetQuery().GetRepoUrl()
	if repo == "" {
		return repo, nil
	}
	if norm, err := gitutil.NormalizeRepoURL(repo); err == nil {
		return norm.String(), nil
	}
	return "", status.InvalidArgumentErrorf("Invalid repo_url: %q", repo)
}

func readPaginatedTargetsFromOLAPDB(ctx context.Context, env environment.Env, req *trpb.GetTargetHistoryRequest) (*trpb.GetTargetHistoryResponse, error) {
	if env.GetOLAPDBHandle() == nil {
		return nil, status.FailedPreconditionError("OLAP database not configured")
	}
	repo, err := getRepoURL(req)
	if err != nil {
		return nil, err
	}
	// Repo URL is required to query OLAP DB for fast query.
	if repo == "" {
		return nil, status.InvalidArgumentError("expected non empty repo_url")
	}

	groupID := req.GetRequestContext().GetGroupId()
	if groupID == "" {
		return nil, status.InvalidArgumentError("expected non empty group_id")
	}

	//  Build the query:
	//  SELECT [a list of fields] FROM "TestTargetStatuses"
	//  WHERE commit_sha IN (
	//    SELECT commit_sha FROM (
	//      SELECT commit_sha, max(invocation_start_time_usec)
	//      as latest_created_at_usec FROM "TestTargetStatuses"
	//      WHERE group_id = '[group_id]'
	//      AND repo_url = '[repo_url]
	//      AND commit_sha != ''
	//      GROUP BY commit_sha
	//      ORDER BY latest_created_at_usec DESC, commit_sha asc)
	//    WHERE (
	//      latest_created_at_usec < [ts_from_pagination_token] OR
	//      (latest_crated_at_usec = [ts_from_pagination_token] AND
	//      commit_sha > '[commit_sha_from_pagitation_token]'))
	//    LIMIT [page_size]
	//  ) AND group_id = '[group_id]'
	//  AND repo_url = '[repo_url']
	//
	// Build the query to select the most recent distinct commits.
	innerCommitQuery := query_builder.NewQuery(`
		SELECT commit_sha, max(invocation_start_time_usec) as latest_created_at_usec 
		FROM "TestTargetStatuses"`)
	innerCommitQuery.AddWhereClause("group_id = ?", groupID)
	innerCommitQuery.AddWhereClause("repo_url = ?", repo)
	innerCommitQuery.SetGroupBy("commit_sha")
	innerCommitQuery.SetOrderBy("latest_created_at_usec DESC, commit_sha", true /*=ascending*/)

	outerCommitQuery := query_builder.NewQuery(`SELECT commit_sha`)
	outerCommitQuery.SetFromClause(innerCommitQuery)
	paginationToken, err := NewTokenFromRequest(req)
	if err != nil {
		return nil, err
	}
	if paginationToken != nil {
		ApplyToQuery("latest_created_at_usec", paginationToken, outerCommitQuery)
	}
	outerCommitQuery.SetLimit(targetHistoryPageSize)

	q := query_builder.NewQuery(`
		SELECT label, rule_type, target_type, test_size, status,
		start_time_usec, duration_usec, invocation_uuid, commit_sha, branch_name,
		repo_url, invocation_start_time_usec
		FROM "TestTargetStatuses"`)

	q.AddWhereInClause("commit_sha", outerCommitQuery)
	q.AddWhereClause("group_id = ?", groupID)
	q.AddWhereClause("repo_url = ?", repo)
	return fetchTargetsFromOLAPDB(ctx, env, q, repo, groupID)
}

func readPaginatedTargetsFromPrimaryDB(ctx context.Context, env environment.Env, req *trpb.GetTargetHistoryRequest) (*trpb.GetTargetHistoryResponse, error) {
	if env.GetDBHandle() == nil {
		return nil, status.FailedPreconditionError("database not configured")
	}

	repo, err := getRepoURL(req)
	if err != nil {
		return nil, err
	}

	// Build the query to select the distinct commits.
	commitQuery := query_builder.NewQuery(`
		SELECT commit_sha, max(created_at_usec) as latest_created_at_usec 
		FROM "Invocations" `)
	commitQuery.AddWhereClause("group_id = ?", req.GetRequestContext().GetGroupId())
	if repo != "" {
		commitQuery.AddWhereClause("repo_url = ?", repo)
	}
	commitQuery.AddWhereClause("role = ?", ciRole)
	commitQuery.AddWhereClause("(command = ? OR command = ?)", testCommand, coverageCommand)
	commitQuery.AddWhereClause("commit_sha != ''")
	paginationToken, err := NewTokenFromRequest(req)
	if err != nil {
		return nil, err
	}
	if paginationToken != nil {
		ApplyToQuery("created_at_usec", paginationToken, commitQuery)
	}

	commitQuery.SetGroupBy("commit_sha")
	commitQuery.SetOrderBy("latest_created_at_usec DESC, commit_sha", true /*=ascending*/)
	commitQuery.SetLimit(targetHistoryPageSize)

	// Build the subquery to select columns from Invocations Table
	joinQuery := query_builder.NewQuery(`
		SELECT invocation_uuid, invocation_id, inv.commit_sha, branch_name, repo_url, 
		created_at_usec 
		FROM "Invocations" as inv`)
	joinQuery.AddJoinClause(commitQuery, "commits", "inv.commit_sha=commits.commit_sha")
	joinQuery.AddWhereClause("inv.group_id = ?", req.GetRequestContext().GetGroupId())
	joinQuery.AddWhereClause("inv.role = ?", ciRole)
	if repo != "" {
		joinQuery.AddWhereClause("inv.repo_url = ?", repo)
	}
	if err := perms.AddPermissionsCheckToQueryWithTableAlias(ctx, env, joinQuery, "inv"); err != nil {
		return nil, err
	}

	// Build the full query to get columns from Targets, TargetStatuses and Invocations
	// Table.
	q := query_builder.NewQuery(`
		SELECT t.target_id, t.label, t.rule_type, ts.target_type, ts.test_size, ts.status,
		ts.start_time_usec, ts.duration_usec, i.invocation_id, i.commit_sha, i.branch_name,
		i.repo_url, i.created_at_usec
		FROM "Targets" as t
		JOIN "TargetStatuses" as ts ON ts.target_id = t.target_id`)
	q.AddJoinClause(joinQuery, "i", "ts.invocation_uuid = i.invocation_uuid")
	return fetchTargetsFromPrimaryDB(ctx, env, q, repo)
}

func isReadFromOLAPDBEnabled(env environment.Env) bool {
	return *readTargetStatusesFromOLAPDBEnabled && env.GetOLAPDBHandle() != nil
}
