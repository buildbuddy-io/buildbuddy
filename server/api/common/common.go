package common

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	cmnpb "github.com/buildbuddy-io/buildbuddy/proto/api/v1/common"
	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

// N.B. This file contains common functions used to format and extract API
// responses from BuildEvents. Changing this code may change the external API,
// so please take care!

// A prefix specifying which ID encoding scheme we're using.
// Don't change this unless you're changing the ID scheme, in which case you should probably check for this
// prefix and support this old ID scheme for some period of time during the migration.
const encodedIDPrefix = "id::v1::"

func EncodeID(id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(encodedIDPrefix + id))
}

func TargetLabelKey(groupID, iid, targetLabel string) string {
	return groupID + "/api/t/" + base64.RawURLEncoding.EncodeToString([]byte(iid+targetLabel))
}

// ActionsKey eturns a string key under which target level actions can be
// recorded in a metrics collector.
func ActionLabelKey(groupID, iid, targetLabel string) string {
	return groupID + "/api/a/" + base64.RawURLEncoding.EncodeToString([]byte(iid+targetLabel))
}

func filesFromOutput(output []*bespb.File) []*apipb.File {
	files := []*apipb.File{}
	for _, output := range output {
		if output == nil {
			continue
		}
		uri := ""
		switch file := output.GetFile().(type) {
		case *bespb.File_Uri:
			uri = file.Uri
			// Contents files are not currently supported - only the file name will be appended without a uri.
		}
		f := &apipb.File{
			Name: output.GetName(),
			Uri:  uri,
		}
		if u, err := url.Parse(uri); err == nil {
			if r, err := digest.ParseDownloadResourceName(u.Path); err == nil {
				f.Hash = r.GetDigest().GetHash()
				f.SizeBytes = r.GetDigest().GetSizeBytes()
			}
		}
		files = append(files, f)
	}
	return files
}

func FillActionFromBuildEvent(event *bespb.BuildEvent, action *apipb.Action) *apipb.Action {
	switch id := event.GetId().GetId().(type) {
	case *bespb.BuildEventId_ActionCompleted:
		action.TargetLabel = id.ActionCompleted.GetLabel()
		action.Id.TargetId = id.ActionCompleted.GetLabel()
		action.Id.ConfigurationId = id.ActionCompleted.GetConfiguration().GetId()
		action.Id.ActionId = EncodeID("build")
		return action
	case *bespb.BuildEventId_TargetCompleted:
		switch p := event.GetPayload().(type) {
		case *bespb.BuildEvent_Completed:
			// Don't create an action for failed targets as they don't have any interesting output files
			// We have already captured them via an ActionCompleted events earlier in the stream.
			if !p.Completed.GetSuccess() {
				return nil
			}
		case *bespb.BuildEvent_Aborted:
			// Don't create an action for aborted targets as they were never built and
			// don't have any interesting output files
			return nil
		}
		action.TargetLabel = id.TargetCompleted.GetLabel()
		action.Id.TargetId = id.TargetCompleted.GetLabel()
		action.Id.ConfigurationId = id.TargetCompleted.GetConfiguration().GetId()
		action.Id.ActionId = EncodeID("build")
		return action
	case *bespb.BuildEventId_TestResult:
		action.TargetLabel = id.TestResult.GetLabel()
		action.Id.TargetId = id.TestResult.GetLabel()
		action.Id.ConfigurationId = id.TestResult.GetConfiguration().GetId()
		action.Id.ActionId = EncodeID(fmt.Sprintf("test-S_%d-R_%d-A_%d", id.TestResult.GetShard(), id.TestResult.GetRun(), id.TestResult.GetAttempt()))
		action.Shard = int64(id.TestResult.GetShard())
		action.Run = int64(id.TestResult.GetRun())
		action.Attempt = int64(id.TestResult.GetAttempt())
		return action
	default:
		return nil
	}
}

func FillActionOutputFilesFromBuildEvent(event *bespb.BuildEvent, action *apipb.Action) *apipb.Action {
	switch p := event.GetPayload().(type) {
	case *bespb.BuildEvent_Action:
		action.File = filesFromOutput([]*bespb.File{p.Action.GetStderr(), p.Action.GetStdout()})
		return action
	case *bespb.BuildEvent_Completed:
		action.File = filesFromOutput(p.Completed.GetDirectoryOutput())
		return action
	case *bespb.BuildEvent_TestResult:
		action.File = filesFromOutput(p.TestResult.GetTestActionOutput())
		return action
	default:
		return nil
	}
}

func TestStatusToStatus(testStatus bespb.TestStatus) cmnpb.Status {
	switch testStatus {
	case bespb.TestStatus_PASSED:
		return cmnpb.Status_PASSED
	case bespb.TestStatus_FLAKY:
		return cmnpb.Status_FLAKY
	case bespb.TestStatus_TIMEOUT:
		return cmnpb.Status_TIMED_OUT
	case bespb.TestStatus_FAILED:
		return cmnpb.Status_FAILED
	case bespb.TestStatus_INCOMPLETE:
		return cmnpb.Status_INCOMPLETE
	case bespb.TestStatus_REMOTE_FAILURE:
		return cmnpb.Status_TOOL_FAILED
	case bespb.TestStatus_FAILED_TO_BUILD:
		return cmnpb.Status_FAILED_TO_BUILD
	case bespb.TestStatus_TOOL_HALTED_BEFORE_TESTING:
		return cmnpb.Status_CANCELLED
	default:
		return cmnpb.Status_STATUS_UNSPECIFIED
	}
}

type TargetMap map[string]*apipb.Target

func (tm TargetMap) ProcessEvent(iid string, event *bespb.BuildEvent) {
	switch p := event.GetPayload().(type) {
	case *bespb.BuildEvent_Configured:
		{
			ruleType := strings.Replace(p.Configured.GetTargetKind(), " rule", "", -1)
			language := ""
			if components := strings.Split(p.Configured.GetTargetKind(), "_"); len(components) > 1 {
				language = components[0]
			}
			label := event.GetId().GetTargetConfigured().GetLabel()
			tm[label] = &apipb.Target{
				Id: &apipb.Target_Id{
					InvocationId: iid,
					TargetId:     label,
				},
				Label:    label,
				Status:   cmnpb.Status_BUILDING,
				RuleType: ruleType,
				Language: language,
				Tag:      p.Configured.GetTag(),
			}
		}
	case *bespb.BuildEvent_Completed:
		{
			if target := tm[event.GetId().GetTargetCompleted().GetLabel()]; target != nil {
				target.Status = cmnpb.Status_BUILT
			}
		}
	case *bespb.BuildEvent_TestSummary:
		{
			if target := tm[event.GetId().GetTestSummary().GetLabel()]; target != nil {
				target.Status = TestStatusToStatus(p.TestSummary.GetOverallStatus())
				target.Timing = TestTimingFromSummary(p.TestSummary)
			}
		}
	}
}

func TestTimingFromSummary(testSummary *bespb.TestSummary) *cmnpb.Timing {
	startTime := timeutil.GetTimeWithFallback(testSummary.GetFirstStartTime(), testSummary.GetFirstStartTimeMillis())
	duration := timeutil.GetDurationWithFallback(testSummary.GetTotalRunDuration(), testSummary.GetTotalRunDurationMillis())
	return &cmnpb.Timing{
		StartTime: timestamppb.New(startTime),
		Duration:  durationpb.New(duration),
	}
}

func TestResultTiming(testResult *bespb.TestResult) *cmnpb.Timing {
	startTime := timeutil.GetTimeWithFallback(testResult.GetTestAttemptStart(), testResult.GetTestAttemptStartMillisEpoch())
	duration := timeutil.GetDurationWithFallback(testResult.GetTestAttemptDuration(), testResult.GetTestAttemptDurationMillis())
	return &cmnpb.Timing{
		StartTime: timestamppb.New(startTime),
		Duration:  durationpb.New(duration),
	}
}

func TargetMapFromInvocation(inv *inpb.Invocation) TargetMap {
	targetMap := make(TargetMap)
	for _, event := range inv.GetEvent() {
		targetMap.ProcessEvent(inv.GetInvocationId(), event.GetBuildEvent())
	}
	return targetMap
}
