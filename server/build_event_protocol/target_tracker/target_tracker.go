package target_tracker

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"flag"
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
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/encoding/prototext"

	cmpb "github.com/buildbuddy-io/buildbuddy/proto/api/v1/common"
)

var enableTargetTracking = flag.Bool("app.enable_target_tracking", false, "Cloud-Only")

type targetClosure func(event *build_event_stream.BuildEvent)
type targetState int

const (
	targetStateExpanded targetState = iota
	targetStateConfigured
	targetStateCompleted
	targetStateResult
	targetStateSummary
	targetStateAborted
)

type target struct {
	label          string
	ruleType       string
	firstStartTime time.Time
	totalDuration  time.Duration
	state          targetState
	id             int64
	overallStatus  build_event_stream.TestStatus
	targetType     cmpb.TargetType
	testSize       build_event_stream.TestSize
	buildSuccess   bool
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

func getTestStatus(aborted *build_event_stream.Aborted) build_event_stream.TestStatus {
	switch aborted.GetReason() {
	case build_event_stream.Aborted_USER_INTERRUPTED:
		return build_event_stream.TestStatus_TOOL_HALTED_BEFORE_TESTING
	default:
		return build_event_stream.TestStatus_FAILED_TO_BUILD
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
		if !p.Completed.GetSuccess() {
			t.overallStatus = build_event_stream.TestStatus_FAILED_TO_BUILD
		}
		t.state = targetStateCompleted
	case *build_event_stream.BuildEvent_TestResult:
		t.state = targetStateResult
	case *build_event_stream.BuildEvent_TestSummary:
		ts := p.TestSummary
		t.overallStatus = ts.GetOverallStatus()
		t.firstStartTime = timeutil.GetTimeWithFallback(ts.GetFirstStartTime(), ts.GetFirstStartTimeMillis())
		t.totalDuration = timeutil.GetDurationWithFallback(ts.GetTotalRunDuration(), ts.GetTotalRunDurationMillis())
		t.state = targetStateSummary
	case *build_event_stream.BuildEvent_Aborted:
		t.buildSuccess = false
		t.state = targetStateAborted
		t.overallStatus = getTestStatus(p.Aborted)
	}
}

func protoID(beid *build_event_stream.BuildEventId) string {
	out, _ := (&prototext.MarshalOptions{Multiline: false}).Marshal(beid)
	return string(out)
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
	buildEventAccumulator accumulator.Accumulator
	targets               map[string]*target
	openClosures          map[string]targetClosure
	errGroup              *errgroup.Group
}

func NewTargetTracker(env environment.Env, buildEventAccumulator accumulator.Accumulator) *TargetTracker {
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

func (t *TargetTracker) testTargetsInAtLeastState(state targetState) bool {
	for _, t := range t.targets {
		if isTest(t) && t.state < state {
			return false
		}
	}
	return true
}

func (t *TargetTracker) permissionsFromContext(ctx context.Context) (*perms.UserGroupPerm, error) {
	if auth := t.env.GetAuthenticator(); auth != nil {
		if u, err := auth.AuthenticatedUser(ctx); err == nil && u.GetGroupID() != "" {
			return perms.GroupAuthPermissions(u.GetGroupID()), nil
		}
	}
	return nil, status.UnauthenticatedError("Context did not contain auth information")
}

func (t *TargetTracker) writeTestTargets(ctx context.Context, permissions *perms.UserGroupPerm) error {
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
			log.Warningf("Error updating %q targets: %s", t.buildEventAccumulator.InvocationID(), err.Error())
			return err
		}
	}
	if len(newTargets) > 0 {
		if err := insertTargets(ctx, t.env, newTargets); err != nil {
			log.Warningf("Error inserting %q targets: %s", t.buildEventAccumulator.InvocationID(), err.Error())
			return err
		}
	}
	return nil
}

func (t *TargetTracker) writeTestTargetStatuses(ctx context.Context, permissions *perms.UserGroupPerm) error {
	repoURL := t.buildEventAccumulator.RepoURL()
	invocationUUID, err := uuid.StringToBytes(t.buildEventAccumulator.InvocationID())
	if err != nil {
		return err
	}
	newTargetStatuses := make([]*tables.TargetStatus, 0)
	for _, target := range t.targets {
		if !isTest(target) {
			continue
		}
		newTargetStatuses = append(newTargetStatuses, &tables.TargetStatus{
			TargetID:       md5Int64(repoURL + target.label),
			InvocationUUID: invocationUUID,
			TargetType:     int32(target.targetType),
			TestSize:       int32(target.testSize),
			Status:         int32(target.overallStatus),
			StartTimeUsec:  target.firstStartTime.UnixMicro(),
			DurationUsec:   target.totalDuration.Microseconds(),
		})
	}
	if err := insertOrUpdateTargetStatuses(ctx, t.env, newTargetStatuses); err != nil {
		log.Warningf("Error inserting %q target statuses: %s", t.buildEventAccumulator.InvocationID(), err.Error())
		return err
	}
	return nil
}

func (t *TargetTracker) TrackTargetsForEvent(ctx context.Context, event *build_event_stream.BuildEvent) {
	if !*enableTargetTracking {
		return
	}
	// Depending on the event type we will either:
	//  - read the set of targets for this repo
	//  - update the set of targets for this repo
	//  - write statuses for the targets at this invocation
	switch event.Payload.(type) {
	case *build_event_stream.BuildEvent_Expanded:
		t.handleExpandedEvent(event)
	case *build_event_stream.BuildEvent_Configured:
		t.handleEvent(event)
	case *build_event_stream.BuildEvent_WorkspaceStatus:
		t.handleWorkspaceStatusEvent(ctx, event)
	case *build_event_stream.BuildEvent_Completed:
		t.handleEvent(event)
	case *build_event_stream.BuildEvent_TestResult:
		t.handleEvent(event)
	case *build_event_stream.BuildEvent_TestSummary:
		t.handleEvent(event)
	case *build_event_stream.BuildEvent_Aborted:
		t.handleEvent(event)
	}

	if event.GetLastMessage() {
		// Handle last message
		t.handleLastEvent(ctx, event)
	}
}

func (t *TargetTracker) handleExpandedEvent(event *build_event_stream.BuildEvent) {
	// This event announces all the upcoming targets we'll get information about.
	// For each target, we'll create a "target" object, keyed by the target label,
	// and each target will "listen" for followup events on the specified ID.
	for _, child := range event.GetChildren() {
		label := child.GetTargetConfigured().GetLabel()
		childTarget := newTarget(label)
		t.targets[label] = childTarget
		t.openClosures[protoID(child)] = childTarget.updateFromEvent
	}
}

func (t *TargetTracker) handleWorkspaceStatusEvent(ctx context.Context, event *build_event_stream.BuildEvent) {
	if !t.testTargetsInAtLeastState(targetStateConfigured) {
		// This should not happen, but it seems it can happen with certain targets.
		// For now, we will log the targets that do not meet the required state
		// so we can better understand whats happening to them.
		log.Warningf("Not all targets for %q reached state: %d, targets: %+v", t.buildEventAccumulator.InvocationID(), targetStateConfigured, t.targets)
		return
	}
	if !isTestCommand(t.buildEventAccumulator.Command()) {
		log.Debugf("Not tracking targets for %q because it's not a test", t.buildEventAccumulator.InvocationID())
		return
	}
	if t.buildEventAccumulator.Role() != "CI" {
		log.Debugf("Not tracking targets for %q because it's not a CI build", t.buildEventAccumulator.InvocationID())
		return
	}
	permissions, err := t.permissionsFromContext(ctx)
	if err != nil {
		log.Debugf("Not tracking targets for %q because it's not authenticated: %s", t.buildEventAccumulator.InvocationID(), err.Error())
		return
	}
	eg, gctx := errgroup.WithContext(ctx)
	t.errGroup = eg
	t.errGroup.Go(func() error { return t.writeTestTargets(gctx, permissions) })
}

func (t *TargetTracker) handleLastEvent(ctx context.Context, event *build_event_stream.BuildEvent) {
	if !isTestCommand(t.buildEventAccumulator.Command()) {
		log.Debugf("Not tracking targets statuses for %q because it's not a test", t.buildEventAccumulator.InvocationID())
		return
	}
	if t.buildEventAccumulator.Role() != "CI" {
		log.Debugf("Not tracking target statuses for %q because it's not a CI build", t.buildEventAccumulator.InvocationID())
		return
	}
	permissions, err := t.permissionsFromContext(ctx)
	if err != nil {
		log.Debugf("Not tracking targets for %q because it's not authenticated: %s", t.buildEventAccumulator.InvocationID(), err.Error())
		return
	}
	if t.errGroup == nil {
		log.Warningf("Not tracking target statuses for %q because targets were not reported", t.buildEventAccumulator.InvocationID())
		return
	}
	// Synchronization point: make sure that all targets were read (or written).
	if err := t.errGroup.Wait(); err != nil {
		log.Warningf("Error getting %q targets: %s", t.buildEventAccumulator.InvocationID(), err.Error())
		return
	}
	if err := t.writeTestTargetStatuses(ctx, permissions); err != nil {
		log.Debugf("Error writing %q target statuses: %s", t.buildEventAccumulator.InvocationID(), err.Error())
	}
}

func isTestCommand(command string) bool {
	return command == "test" || command == "coverage"
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
			nowUsec := time.Now().UnixMicro()
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
		err := env.GetDBHandle().TransactionWithOptions(ctx, db.Opts().WithQueryName("target_tracker_insert_targets"), func(tx *db.DB) error {
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
	if len(items) == 0 {
		return nil
	}
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
			nowUsec := time.Now().UnixMicro()
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?)")
			valueArgs = append(valueArgs, t.TargetID)
			valueArgs = append(valueArgs, t.InvocationUUID)
			valueArgs = append(valueArgs, t.TargetType)
			valueArgs = append(valueArgs, t.TestSize)
			valueArgs = append(valueArgs, t.Status)
			valueArgs = append(valueArgs, t.StartTimeUsec)
			valueArgs = append(valueArgs, t.DurationUsec)
			valueArgs = append(valueArgs, nowUsec)
			valueArgs = append(valueArgs, nowUsec)
		}
		err := env.GetDBHandle().TransactionWithOptions(ctx, db.Opts().WithQueryName("target_tracker_insert_target_statuses"), func(tx *db.DB) error {
			stmt := fmt.Sprintf("INSERT INTO TargetStatuses (target_id, invocation_uuid, target_type, test_size, status, start_time_usec, duration_usec, created_at_usec, updated_at_usec) VALUES %s", strings.Join(valueStrings, ","))
			return tx.Exec(stmt, valueArgs...).Error
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func TargetTrackingEnabled() bool {
	return *enableTargetTracking
}
