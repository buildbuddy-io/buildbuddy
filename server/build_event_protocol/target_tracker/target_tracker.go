package target_tracker

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/accumulator"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/proto"
	"github.com/jinzhu/gorm"

	cmpb "github.com/buildbuddy-io/buildbuddy/proto/api/v1/common"
	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
)

type targetClosure func(event *build_event_stream.BuildEvent)
type targetState int

const (
	targetStateExpanded targetState = iota
	targetStateConfigured
	targetStateCompleted
	targetStateResult
	targetStateSummary
)

type result struct {
	cachedLocally  bool
	status         build_event_stream.TestStatus
	startMillis    int64
	durationMillis int64
}
type target struct {
	id                  int64
	label               string
	ruleType            string
	testSize            build_event_stream.TestSize
	buildSuccess        bool
	results             []*result
	overallStatus       build_event_stream.TestStatus
	firstStartMillis    int64
	lastStopMillis      int64
	totalDurationMillis int64

	state targetState
}

func md5Int64(text string) int64 {
	hash := md5.Sum([]byte(text))
	return int64(binary.BigEndian.Uint64(hash[:8]))
}

func newTarget(label string) *target {
	return &target{
		id:    md5Int64(label),
		label: label,
		state: targetStateExpanded,
	}
}

func (t *target) updateFromEvent(event *build_event_stream.BuildEvent) {
	switch p := event.Payload.(type) {
	case *build_event_stream.BuildEvent_Configured:
		t.ruleType = p.Configured.GetTargetKind()
		t.testSize = p.Configured.GetTestSize()
		t.state = targetStateConfigured
	case *build_event_stream.BuildEvent_Completed:
		t.buildSuccess = p.Completed.GetSuccess()
		t.state = targetStateCompleted
	case *build_event_stream.BuildEvent_TestResult:
		t.results = append(t.results, &result{
			cachedLocally:  p.TestResult.GetCachedLocally(),
			status:         p.TestResult.GetStatus(),
			startMillis:    p.TestResult.GetTestAttemptStartMillisEpoch(),
			durationMillis: p.TestResult.GetTestAttemptDurationMillis(),
		})
		t.state = targetStateResult
	case *build_event_stream.BuildEvent_TestSummary:
		t.overallStatus = p.TestSummary.GetOverallStatus()
		t.firstStartMillis = p.TestSummary.GetFirstStartTimeMillis()
		t.lastStopMillis = p.TestSummary.GetLastStopTimeMillis()
		t.totalDurationMillis = p.TestSummary.GetTotalRunDurationMillis()
		t.state = targetStateSummary
	}
}

func protoID(beid *build_event_stream.BuildEventId) string {
	return proto.CompactTextString(beid)
}

func targetTypeFromRuleType(ruleType string) cmpb.TargetType {
	ruleType = strings.TrimSuffix(ruleType, " rule")
	switch {
	case strings.HasSuffix(ruleType, "application"):
		return cmpb.TargetType_APPLICATION
	case strings.HasSuffix(ruleType, "binary"):
		return cmpb.TargetType_BINARY
	case strings.HasSuffix(ruleType, "library"):
		return cmpb.TargetType_LIBRARY
	case strings.HasSuffix(ruleType, "package"):
		return cmpb.TargetType_PACKAGE
	case strings.HasSuffix(ruleType, "test"):
		return cmpb.TargetType_TEST
	default:
		return cmpb.TargetType_TARGET_TYPE_UNSPECIFIED
	}
}

type TargetTracker struct {
	env                   environment.Env
	buildEventAccumulator *accumulator.BEValues
	targets               map[string]*target
	openClosures          map[string]targetClosure
}

func NewTargetTracker(env environment.Env, buildEventAccumulator *accumulator.BEValues) *TargetTracker {
	return &TargetTracker{
		env:                   env,
		buildEventAccumulator: buildEventAccumulator,
		targets:               make(map[string]*target, 0),
		openClosures:          make(map[string]targetClosure, 0),
	}
}

func (t *TargetTracker) handleEvent(event *build_event_stream.BuildEvent) {
	id := protoID(event.GetId())
	openClosure, ok := t.openClosures[id]
	if !ok {
		return
	}
	delete(t.openClosures, id)
	openClosure(event)
	for _, child := range event.GetChildren() {
		t.openClosures[protoID(child)] = openClosure
	}
}

func isTest(t *target) bool {
	if t.testSize != build_event_stream.TestSize_UNKNOWN {
		return true
	}
	return strings.HasSuffix(strings.ToLower(t.ruleType), "test")
}

func (t *TargetTracker) testTargetsInState(state targetState) bool {
	for _, t := range t.targets {
		if isTest(t) && t.state != state {
			return false
		}
	}
	return true
}

func (t *TargetTracker) allTargetsInState(state targetState) bool {
	for _, t := range t.targets {
		if t.state != state {
			return false
		}
	}
	return true
}

func (t *TargetTracker) writeAllTargets(ctx context.Context) {
	var permissions *perms.UserGroupPerm
	if auth := t.env.GetAuthenticator(); auth != nil {
		if u, err := auth.AuthenticatedUser(ctx); err == nil && u.GetGroupID() != "" {
			permissions = perms.GroupAuthPermissions(u.GetGroupID())
		}
	}
	if permissions == nil {
		log.Printf("Permissions were nil -- not writing target data.")
		return
	}
	repoURL := t.buildEventAccumulator.RepoURL()
	knownTargets, err := readRepoTargets(ctx, t.env, repoURL)
	if err != nil {
		log.Printf("error reading targets: %s", err.Error())
	}
	knownTargetsByLabel := make(map[string]*tables.Target, len(knownTargets))
	for _, knownTarget := range knownTargets {
		knownTargetsByLabel[knownTarget.Label] = knownTarget
	}
	newTargets := make([]*target, 0)
	for label, target := range t.targets {
		// Is this already in the DB?
		_, ok := knownTargetsByLabel[label]
		if !ok {
			newTargets = append(newTargets, target)
		} else {
			log.Printf("Target %s was already known!", label)
		}
	}
	if len(newTargets) > 0 {
		tableTargets := make([]*tables.Target, 0, len(newTargets))
		for _, target := range newTargets {
			tableTargets = append(tableTargets, &tables.Target{
				RepoURL:  repoURL,
				TargetID: target.id,
				UserID:   permissions.UserID,
				GroupID:  permissions.GroupID,
				Perms:    permissions.Perms,
				Label:    target.label,
				RuleType: target.ruleType,
			})
		}
		if err := insertOrUpdateTargets(ctx, t.env, tableTargets); err != nil {
			log.Printf("Error inserting targets: %s", err.Error())
		}
	}
}

func (t *TargetTracker) writeTestTargetStatuses(ctx context.Context) {
	var permissions *perms.UserGroupPerm
	if auth := t.env.GetAuthenticator(); auth != nil {
		if u, err := auth.AuthenticatedUser(ctx); err == nil && u.GetGroupID() != "" {
			permissions = perms.GroupAuthPermissions(u.GetGroupID())
		}
	}
	if permissions == nil {
		log.Printf("Permissions were nil -- not writing target data.")
		return
	}
	invocationPK := md5Int64(t.buildEventAccumulator.InvocationID())
	newTargetStatuses := make([]*tables.TargetStatus, 0)
	for _, target := range t.targets {
		if !isTest(target) {
			continue
		}
		newTargetStatuses = append(newTargetStatuses, &tables.TargetStatus{
			TargetID:      target.id,
			InvocationPK:  invocationPK,
			TargetType:    int32(targetTypeFromRuleType(target.ruleType)),
			TestSize:      int32(target.testSize),
			Status:        int32(target.overallStatus),
			StartTimeUsec: int64(target.firstStartMillis * 1000),
			DurationUsec:  int64(target.totalDurationMillis * 1000),
		})
	}
	if err := insertOrUpdateTargetStatuses(ctx, t.env, newTargetStatuses); err != nil {
		log.Printf("Error inserting target statuses: %s", err.Error())
	}
}

func (t *TargetTracker) TrackTargetsForEvent(ctx context.Context, event *build_event_stream.BuildEvent) {
	// Depending on the event type we will either:
	//  - read the set of targets for this repo
	//  - update the set of targets for this repo
	//  - write statuses for the targets at this invocation
	switch event.Payload.(type) {
	case *build_event_stream.BuildEvent_Expanded:
		// This event announces all the upcoming targets we'll get information about.
		// For each target, we'll create a "target" object, keyed by the target label,
		// and each target will "listen" for followup events on the specified ID.
		for _, child := range event.GetChildren() {
			label := child.GetTargetConfigured().GetLabel()
			childTarget := newTarget(label)
			t.targets[label] = childTarget
			t.openClosures[protoID(child)] = childTarget.updateFromEvent
		}
	case *build_event_stream.BuildEvent_Configured:
		t.handleEvent(event)
		if t.allTargetsInState(targetStateConfigured) {
			if t.buildEventAccumulator.Role() == "CI" {
				t.writeAllTargets(ctx)
			}
		}
	case *build_event_stream.BuildEvent_Completed:
		t.handleEvent(event)
		if t.allTargetsInState(targetStateCompleted) {
		}
	case *build_event_stream.BuildEvent_TestResult:
		t.handleEvent(event)
	case *build_event_stream.BuildEvent_TestSummary:
		t.handleEvent(event)
		if t.testTargetsInState(targetStateSummary) {
			if t.buildEventAccumulator.Role() == "CI" {
				t.writeTestTargetStatuses(ctx)
			}
		}
	}
}

func readRepoTargetsWithTx(ctx context.Context, env environment.Env, repoURL string, tx *gorm.DB) ([]*tables.Target, error) {
	q := query_builder.NewQuery(`SELECT * FROM Targets as t`)
	q = q.AddWhereClause(`t.repo_url = ?`, repoURL)
	if err := perms.AddPermissionsCheckToQueryWithTableAlias(ctx, env, q, "t"); err != nil {
		return nil, err
	}
	queryStr, args := q.Build()
	rows, err := tx.Raw(queryStr, args...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rsp := make([]*tables.Target, 0)
	for rows.Next() {
		t := &tables.Target{}
		if err := tx.ScanRows(rows, t); err != nil {
			return nil, err
		}
		rsp = append(rsp, t)
	}
	return rsp, nil
}

func readRepoTargets(ctx context.Context, env environment.Env, repoURL string) ([]*tables.Target, error) {
	if env.GetDBHandle() == nil {
		return nil, status.FailedPreconditionError("database not configured")
	}
	var err error
	rsp := make([]*tables.Target, 0)
	err = env.GetDBHandle().Transaction(func(tx *gorm.DB) error {
		rsp, err = readRepoTargetsWithTx(ctx, env, repoURL, tx)
		return err
	})
	if err != nil {
		return nil, err
	}
	return rsp, nil
}

func chunkTargetsBy(items []*tables.Target, chunkSize int) (chunks [][]*tables.Target) {
	for chunkSize < len(items) {
		items, chunks = items[chunkSize:], append(chunks, items[0:chunkSize:chunkSize])
	}
	return append(chunks, items)
}

func insertOrUpdateTargets(ctx context.Context, env environment.Env, targets []*tables.Target) error {
	if env.GetDBHandle() == nil {
		return status.FailedPreconditionError("database not configured")
	}
	chunkList := chunkTargetsBy(targets, 100)
	for _, chunk := range chunkList {
		valueStrings := []string{}
		valueArgs := []interface{}{}
		for _, t := range chunk {
			nowInt64 := int64(time.Now().UnixNano() / 1000)
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, t.RepoURL)
			valueArgs = append(valueArgs, t.TargetID)
			valueArgs = append(valueArgs, t.UserID)
			valueArgs = append(valueArgs, t.GroupID)
			valueArgs = append(valueArgs, t.Perms)
			valueArgs = append(valueArgs, t.Label)
			valueArgs = append(valueArgs, t.RuleType)
			valueArgs = append(valueArgs, nowInt64)
			valueArgs = append(valueArgs, nowInt64)
		}
		err := env.GetDBHandle().Transaction(func(tx *gorm.DB) error {
			stmt := fmt.Sprintf("INSERT INTO Targets (repo_url, target_id, user_id, group_id, perms, label, rule_type, created_at_usec, updated_at_usec) VALUES %s", strings.Join(valueStrings, ","))
			return tx.Exec(stmt, valueArgs...).Error
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func chunkStatusesBy(items []*tables.TargetStatus, chunkSize int) (chunks [][]*tables.TargetStatus) {
	for chunkSize < len(items) {
		items, chunks = items[chunkSize:], append(chunks, items[0:chunkSize:chunkSize])
	}
	return append(chunks, items)
}

func insertOrUpdateTargetStatuses(ctx context.Context, env environment.Env, statuses []*tables.TargetStatus) error {
	if env.GetDBHandle() == nil {
		return status.FailedPreconditionError("database not configured")
	}
	chunkList := chunkStatusesBy(statuses, 100)
	for _, chunk := range chunkList {
		valueStrings := []string{}
		valueArgs := []interface{}{}
		for _, t := range chunk {
			nowInt64 := int64(time.Now().UnixNano() / 1000)
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, t.TargetID)
			valueArgs = append(valueArgs, t.InvocationPK)
			valueArgs = append(valueArgs, t.TargetType)
			valueArgs = append(valueArgs, t.TestSize)
			valueArgs = append(valueArgs, t.Status)
			valueArgs = append(valueArgs, t.StartTimeUsec)
			valueArgs = append(valueArgs, t.DurationUsec)
			valueArgs = append(valueArgs, nowInt64)
			valueArgs = append(valueArgs, nowInt64)
		}
		err := env.GetDBHandle().Transaction(func(tx *gorm.DB) error {
			stmt := fmt.Sprintf("INSERT INTO TargetStatuses (target_id, invocation_pk, target_type, test_size, status, start_time_usec, duration_usec, created_at_usec, updated_at_usec) VALUES %s", strings.Join(valueStrings, ","))
			return tx.Exec(stmt, valueArgs...).Error
		})
		if err != nil {
			return err
		}
	}
	return nil
}
