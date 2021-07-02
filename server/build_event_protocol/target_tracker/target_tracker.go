package target_tracker

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/accumulator"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"
	"github.com/golang/protobuf/proto"
	"golang.org/x/sync/errgroup"

	cmpb "github.com/buildbuddy-io/buildbuddy/proto/api/v1/common"
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
	label               string
	ruleType            string
	results             []*result
	firstStartMillis    int64
	totalDurationMillis int64
	state               targetState
	id                  int64
	lastStopMillis      int64
	overallStatus       build_event_stream.TestStatus
	targetType          cmpb.TargetType
	testSize            build_event_stream.TestSize
	buildSuccess        bool
}

func md5Int64(text string) int64 {
	hash := md5.Sum([]byte(text))
	return int64(binary.BigEndian.Uint64(hash[:8]))
}

func newTarget(label string) *target {
	return &target{
		label: label,
		state: targetStateExpanded,
	}
}

func (t *target) updateFromEvent(event *build_event_stream.BuildEvent) {
	switch p := event.Payload.(type) {
	case *build_event_stream.BuildEvent_Configured:
		t.ruleType = p.Configured.GetTargetKind()
		t.targetType = targetTypeFromRuleType(t.ruleType)
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
	errGroup              *errgroup.Group
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

func (t *TargetTracker) testTargetsInAtleastState(state targetState) bool {
	for _, t := range t.targets {
		if isTest(t) && t.state < state {
			return false
		}
	}
	return true
}

func (t *TargetTracker) writeTestTargets(ctx context.Context) error {
	var permissions *perms.UserGroupPerm
	if auth := t.env.GetAuthenticator(); auth != nil {
		if u, err := auth.AuthenticatedUser(ctx); err == nil && u.GetGroupID() != "" {
			permissions = perms.GroupAuthPermissions(u.GetGroupID())
		}
	}
	if permissions == nil {
		return status.FailedPreconditionError("Permissions were nil -- not writing target data.")
	}
	repoURL := t.buildEventAccumulator.RepoURL()
	knownTargets, err := readRepoTargets(ctx, t.env, repoURL)
	if err != nil {
		return err
	}
	knownTargetsByLabel := make(map[string]*tables.Target, len(knownTargets))
	for _, knownTarget := range knownTargets {
		knownTargetsByLabel[knownTarget.Label] = knownTarget
	}
	newTargets := make([]*tables.Target, 0)
	updatedTargets := make([]*tables.Target, 0)
	for label, target := range t.targets {
		if target.targetType != cmpb.TargetType_TEST {
			continue
		}
		tableTarget := &tables.Target{
			RepoURL:  repoURL,
			TargetID: md5Int64(repoURL + target.label),
			UserID:   permissions.UserID,
			GroupID:  permissions.GroupID,
			Perms:    permissions.Perms,
			Label:    target.label,
			RuleType: target.ruleType,
		}
		knownTarget, ok := knownTargetsByLabel[label]
		if !ok {
			newTargets = append(newTargets, tableTarget)
			continue
		}
		if knownTarget.RuleType != target.ruleType {
			updatedTargets = append(updatedTargets, tableTarget)
			continue
		}
	}
	if len(updatedTargets) > 0 {
		if err := updateTargets(ctx, t.env, updatedTargets); err != nil {
			log.Warningf("Error updating targets: %s", err.Error())
			return err
		}
	}
	if len(newTargets) > 0 {
		if err := insertTargets(ctx, t.env, newTargets); err != nil {
			log.Warningf("Error inserting targets: %s", err.Error())
			return err
		}
	}
	return nil
}

func (t *TargetTracker) writeTestTargetStatuses(ctx context.Context) error {
	var permissions *perms.UserGroupPerm
	if auth := t.env.GetAuthenticator(); auth != nil {
		if u, err := auth.AuthenticatedUser(ctx); err == nil && u.GetGroupID() != "" {
			permissions = perms.GroupAuthPermissions(u.GetGroupID())
		}
	}
	if permissions == nil {
		return status.FailedPreconditionError("Permissions were nil -- not writing target data.")
	}
	repoURL := t.buildEventAccumulator.RepoURL()
	invocationPK := md5Int64(t.buildEventAccumulator.InvocationID())
	newTargetStatuses := make([]*tables.TargetStatus, 0)
	for _, target := range t.targets {
		if !isTest(target) {
			continue
		}
		newTargetStatuses = append(newTargetStatuses, &tables.TargetStatus{
			TargetID:      md5Int64(repoURL + target.label),
			InvocationPK:  invocationPK,
			TargetType:    int32(target.targetType),
			TestSize:      int32(target.testSize),
			Status:        int32(target.overallStatus),
			StartTimeUsec: int64(target.firstStartMillis * 1000),
			DurationUsec:  int64(target.totalDurationMillis * 1000),
		})
	}
	if err := insertOrUpdateTargetStatuses(ctx, t.env, newTargetStatuses); err != nil {
		log.Warningf("Error inserting target statuses: %s", err.Error())
		return err
	}
	return nil
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
	case *build_event_stream.BuildEvent_WorkspaceStatus:
		if t.testTargetsInAtleastState(targetStateConfigured) {
			if t.buildEventAccumulator.Role() == "CI" {
				eg, gctx := errgroup.WithContext(ctx)
				t.errGroup = eg
				t.errGroup.Go(func() error { return t.writeTestTargets(gctx) })
			}
		} else {
			// This should not happen, but it seems it can happen with certain targets.
			// For now, we will log the targets that do not meet the required state
			// so we can better understand whats happening to them.
			log.Printf("Not all targets reached state: %d, targets: %+v", targetStateConfigured, t.targets)
		}
	case *build_event_stream.BuildEvent_Completed:
		t.handleEvent(event)
	case *build_event_stream.BuildEvent_TestResult:
		t.handleEvent(event)
	case *build_event_stream.BuildEvent_TestSummary:
		t.handleEvent(event)
		if !t.testTargetsInAtleastState(targetStateSummary) || t.buildEventAccumulator.Role() != "CI" || t.errGroup == nil {
			break
		}
		// Synchronization point: make sure that all targets were read (or written).
		if err := t.errGroup.Wait(); err != nil {
			log.Printf("Error getting targets: %s", err.Error())
			break
		}
		eg, gctx := errgroup.WithContext(ctx)
		t.errGroup = eg
		t.errGroup.Go(func() error { return t.writeTestTargetStatuses(gctx) })
	case *build_event_stream.BuildEvent_Finished:
		if t.errGroup == nil {
			break
		}
		// Synchronization point: make sure that all statuses were written.
		if err := t.errGroup.Wait(); err != nil {
			// Debug because this logs when unauthorized/non-CI builds skip writing targets.
			log.Debugf("Error writing target statuses: %s", err.Error())
		}
	}
}

func readRepoTargetsWithTx(ctx context.Context, env environment.Env, repoURL string, tx *db.DB) ([]*tables.Target, error) {
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
	err = env.GetDBHandle().Transaction(ctx, func(tx *db.DB) error {
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

func updateTargets(ctx context.Context, env environment.Env, targets []*tables.Target) error {
	if env.GetDBHandle() == nil {
		return status.FailedPreconditionError("database not configured")
	}
	for _, t := range targets {
		err := env.GetDBHandle().Transaction(ctx, func(tx *db.DB) error {
			var existing tables.Target
			if err := tx.Where("target_id = ?", t.TargetID).First(&existing).Error; err != nil {
				return err
			}
			return tx.Model(&existing).Where("target_id = ?", t.TargetID).Updates(t).Error
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func insertTargets(ctx context.Context, env environment.Env, targets []*tables.Target) error {
	if env.GetDBHandle() == nil {
		return status.FailedPreconditionError("database not configured")
	}
	chunkList := chunkTargetsBy(targets, 100)
	for _, chunk := range chunkList {
		valueStrings := []string{}
		valueArgs := []interface{}{}
		for _, t := range chunk {
			nowUsec := timeutil.ToUsec(time.Now())
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, t.RepoURL)
			valueArgs = append(valueArgs, t.TargetID)
			valueArgs = append(valueArgs, t.UserID)
			valueArgs = append(valueArgs, t.GroupID)
			valueArgs = append(valueArgs, t.Perms)
			valueArgs = append(valueArgs, t.Label)
			valueArgs = append(valueArgs, t.RuleType)
			valueArgs = append(valueArgs, nowUsec)
			valueArgs = append(valueArgs, nowUsec)
		}
		err := env.GetDBHandle().Transaction(ctx, func(tx *db.DB) error {
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
			nowUsec := timeutil.ToUsec(time.Now())
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, t.TargetID)
			valueArgs = append(valueArgs, t.InvocationPK)
			valueArgs = append(valueArgs, t.TargetType)
			valueArgs = append(valueArgs, t.TestSize)
			valueArgs = append(valueArgs, t.Status)
			valueArgs = append(valueArgs, t.StartTimeUsec)
			valueArgs = append(valueArgs, t.DurationUsec)
			valueArgs = append(valueArgs, nowUsec)
			valueArgs = append(valueArgs, nowUsec)
		}
		err := env.GetDBHandle().Transaction(ctx, func(tx *db.DB) error {
			stmt := fmt.Sprintf("INSERT INTO TargetStatuses (target_id, invocation_pk, target_type, test_size, status, start_time_usec, duration_usec, created_at_usec, updated_at_usec) VALUES %s", strings.Join(valueStrings, ","))
			return tx.Exec(stmt, valueArgs...).Error
		})
		if err != nil {
			return err
		}
	}
	return nil
}
