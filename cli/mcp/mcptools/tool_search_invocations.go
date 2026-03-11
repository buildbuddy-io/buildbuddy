package mcptools

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	osuser "os/user"
	"strconv"
	"strings"
	"time"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	gstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// Default SearchInvocation count when count is explicitly requested with fallback.
	defaultSearchCount = 20
	// Hard cap for SearchInvocation result count.
	maxSearchCount = 100
	// Default lookback window (days) when lookback is explicitly requested with fallback.
	defaultLookbackDays = 7
	// Hard cap for lookback days in search_invocations.
	maxLookbackDays = 365
)

func (s *Service) searchInvocations(ctx context.Context, args map[string]any) (any, error) {
	userValue, err := optionalString(args, "user")
	if err != nil {
		return nil, err
	}
	allUsers, err := optionalBool(args, "all_users", false)
	if err != nil {
		return nil, err
	}
	repoURL, err := optionalString(args, "repo_url")
	if err != nil {
		return nil, err
	}
	branchName, err := optionalString(args, "branch_name")
	if err != nil {
		return nil, err
	}
	command, err := optionalString(args, "command")
	if err != nil {
		return nil, err
	}
	pattern, err := optionalString(args, "pattern")
	if err != nil {
		return nil, err
	}
	workflowActionName, err := optionalString(args, "workflow_action_name")
	if err != nil {
		return nil, err
	}
	if pattern != "" && workflowActionName != "" {
		return nil, fmt.Errorf("\"pattern\" and \"workflow_action_name\" cannot both be set")
	}
	if workflowActionName != "" {
		pattern = workflowActionName
	}
	role, err := optionalString(args, "role")
	if err != nil {
		return nil, err
	}
	pageToken, err := optionalString(args, "page_token")
	if err != nil {
		return nil, err
	}
	count := 0
	hasCount := false
	if _, ok := args["count"]; ok {
		count, err = optionalPositiveInt(args, "count", defaultSearchCount, maxSearchCount)
		if err != nil {
			return nil, err
		}
		hasCount = true
	}
	lookbackDays := 0
	hasLookbackDays := false
	if _, ok := args["lookback_days"]; ok {
		lookbackDays, err = optionalPositiveInt(args, "lookback_days", defaultLookbackDays, maxLookbackDays)
		if err != nil {
			return nil, err
		}
		hasLookbackDays = true
	}
	sortFieldValue, hasSortField := args["sort_field"]
	sortField := inpb.InvocationSort_UPDATED_AT_USEC_SORT_FIELD
	if hasSortField {
		parsedSortField, err := parseInvocationSortField(sortFieldValue)
		if err != nil {
			return nil, err
		}
		sortField = parsedSortField
	}
	ascending, err := optionalBool(args, "ascending", false)
	if err != nil {
		return nil, err
	}
	updatedAfter, err := optionalTimestampString(args, "updated_after")
	if err != nil {
		return nil, err
	}
	updatedBefore, err := optionalTimestampString(args, "updated_before")
	if err != nil {
		return nil, err
	}

	query := &inpb.InvocationQuery{
		RepoUrl:    repoURL,
		BranchName: branchName,
		Command:    command,
		Pattern:    pattern,
	}
	if role != "" {
		query.Role = []string{role}
	}

	userFilterSource := "none"
	if userValue != "" {
		query.User = userValue
		userFilterSource = "user"
	} else if !allUsers {
		if currentUser := currentLinuxUsername(); currentUser != "" {
			query.User = currentUser
			userFilterSource = "current_linux_username"
		}
	}

	lookbackSource := "none"
	if updatedAfter != nil {
		query.UpdatedAfter = updatedAfter
		lookbackSource = "updated_after"
	} else if hasLookbackDays {
		query.UpdatedAfter = timestamppb.New(time.Now().AddDate(0, 0, -lookbackDays))
		lookbackSource = "lookback_days"
	}
	if updatedBefore != nil {
		query.UpdatedBefore = updatedBefore
	}
	if query.GetUpdatedAfter() != nil && query.GetUpdatedBefore() != nil {
		if query.GetUpdatedBefore().AsTime().Before(query.GetUpdatedAfter().AsTime()) {
			return nil, fmt.Errorf("\"updated_before\" must be on or after \"updated_after\"")
		}
	}

	authCtx, err := s.authenticatedContext(ctx)
	if err != nil {
		return nil, err
	}
	countValue := any(nil)
	if hasCount {
		countValue = count
	}
	searchReq := &inpb.SearchInvocationRequest{
		Query: query,
		Sort: &inpb.InvocationSort{
			SortField: sortField,
			Ascending: ascending,
		},
		Count:     int32(count),
		PageToken: pageToken,
	}
	rsp, err := s.bbClient.SearchInvocation(authCtx, searchReq)
	if err != nil {
		if gstatus.Code(err).String() == "DeadlineExceeded" || errors.Is(err, context.DeadlineExceeded) {
			return nil, apiRequestError(
				"search invocations timed out; this is often an MCP tool timeout rather than an API limitation. Try increasing MCP tool timeout and retrying this same query",
				searchReq,
				err,
			)
		}
		return nil, apiRequestError("search invocations", searchReq, err)
	}

	invocationSummaries := make([]map[string]any, 0, len(rsp.GetInvocation()))
	totalDurationUsec := int64(0)
	for _, inv := range rsp.GetInvocation() {
		invocationSummaries = append(invocationSummaries, invocationMetadata(inv))
		totalDurationUsec += inv.GetDurationUsec()
	}
	averageDurationUsec := int64(0)
	if len(rsp.GetInvocation()) > 0 {
		averageDurationUsec = totalDurationUsec / int64(len(rsp.GetInvocation()))
	}

	lookbackDaysValue := any(nil)
	if hasLookbackDays {
		lookbackDaysValue = lookbackDays
	}
	appliedQuery := map[string]any{
		"user":                 query.GetUser(),
		"user_filter":          userFilterSource,
		"repo_url":             query.GetRepoUrl(),
		"branch_name":          query.GetBranchName(),
		"command":              query.GetCommand(),
		"pattern":              query.GetPattern(),
		"workflow_action_name": workflowActionName,
		"role":                 role,
		"updated_after":        timestampString(query.GetUpdatedAfter()),
		"updated_before":       timestampString(query.GetUpdatedBefore()),
		"lookback_days":        lookbackDaysValue,
		"lookback_source":      lookbackSource,
		"sort_field":           sortField.String(),
		"ascending":            ascending,
		"count":                countValue,
		"page_token":           pageToken,
	}

	return map[string]any{
		"query":                 appliedQuery,
		"invocation_count":      len(invocationSummaries),
		"next_page_token":       rsp.GetNextPageToken(),
		"average_duration_usec": averageDurationUsec,
		"invocations":           invocationSummaries,
	}, nil
}

func parseInvocationSortField(value any) (inpb.InvocationSort_SortField, error) {
	switch typed := value.(type) {
	case float64:
		if math.Trunc(typed) != typed {
			return inpb.InvocationSort_UNKNOWN_SORT_FIELD, fmt.Errorf("\"sort_field\" must be an integer or sort field name")
		}
		n := int32(typed)
		sortField := inpb.InvocationSort_SortField(n)
		if _, ok := inpb.InvocationSort_SortField_name[n]; ok {
			return sortField, nil
		}
		return inpb.InvocationSort_UNKNOWN_SORT_FIELD, fmt.Errorf("unknown sort_field numeric value %d", n)
	case string:
		s := strings.TrimSpace(typed)
		if s == "" {
			return inpb.InvocationSort_UNKNOWN_SORT_FIELD, fmt.Errorf("\"sort_field\" must be non-empty")
		}
		if n, err := strconv.Atoi(s); err == nil {
			sortField := inpb.InvocationSort_SortField(n)
			if _, ok := inpb.InvocationSort_SortField_name[int32(sortField)]; ok {
				return sortField, nil
			}
			return inpb.InvocationSort_UNKNOWN_SORT_FIELD, fmt.Errorf("unknown sort_field numeric value %d", n)
		}

		normalized := strings.ToUpper(strings.ReplaceAll(strings.ReplaceAll(s, "-", "_"), " ", "_"))
		if value, ok := inpb.InvocationSort_SortField_value[normalized]; ok {
			return inpb.InvocationSort_SortField(value), nil
		}
		if value, ok := inpb.InvocationSort_SortField_value[normalized+"_SORT_FIELD"]; ok {
			return inpb.InvocationSort_SortField(value), nil
		}
		return inpb.InvocationSort_UNKNOWN_SORT_FIELD, fmt.Errorf("unknown sort_field %q", s)
	default:
		return inpb.InvocationSort_UNKNOWN_SORT_FIELD, fmt.Errorf("\"sort_field\" must be an integer or sort field name")
	}
}

func optionalTimestampString(args map[string]any, field string) (*timestamppb.Timestamp, error) {
	value, err := optionalString(args, field)
	if err != nil {
		return nil, err
	}
	if value == "" {
		return nil, nil
	}
	t, err := parseTimestampString(value)
	if err != nil {
		return nil, fmt.Errorf("parse %q: %w", field, err)
	}
	return timestamppb.New(t), nil
}

func parseTimestampString(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, fmt.Errorf("timestamp is empty")
	}
	if t, err := time.Parse(time.RFC3339, value); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02", value); err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("expected RFC3339 or YYYY-MM-DD")
}

func currentLinuxUsername() string {
	if u, err := osuser.Current(); err == nil {
		username := strings.TrimSpace(u.Username)
		if username != "" {
			if parts := strings.Split(username, `\`); len(parts) > 0 {
				username = parts[len(parts)-1]
			}
			return username
		}
	}
	if username := strings.TrimSpace(os.Getenv("USER")); username != "" {
		return username
	}
	if username := strings.TrimSpace(os.Getenv("LOGNAME")); username != "" {
		return username
	}
	return ""
}
