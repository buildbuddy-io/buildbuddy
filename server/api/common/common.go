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
		uri := ""
		switch file := output.File.(type) {
		case *bespb.File_Uri:
			uri = file.Uri
			// Contents files are not currently supported - only the file name will be appended without a uri.
		}
		f := &apipb.File{
			Name: output.Name,
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
	switch event.Payload.(type) {
	case *bespb.BuildEvent_Completed:
		{
			action.TargetLabel = event.GetId().GetTargetCompleted().GetLabel()
			action.Id.TargetId = event.GetId().GetTargetCompleted().GetLabel()
			action.Id.ConfigurationId = event.GetId().GetTargetCompleted().GetConfiguration().Id
			action.Id.ActionId = EncodeID("build")
			return action
		}
	case *bespb.BuildEvent_TestResult:
		{
			testResultID := event.GetId().GetTestResult()
			action.TargetLabel = event.GetId().GetTestResult().GetLabel()
			action.Id.TargetId = event.GetId().GetTestResult().GetLabel()
			action.Id.ConfigurationId = event.GetId().GetTestResult().GetConfiguration().Id
			action.Id.ActionId = EncodeID(fmt.Sprintf("test-S_%d-R_%d-A_%d", testResultID.Shard, testResultID.Run, testResultID.Attempt))
			return action
		}
	}
	return nil
}

func FillActionOutputFilesFromBuildEvent(event *bespb.BuildEvent, action *apipb.Action) *apipb.Action {
	switch p := event.Payload.(type) {
	case *bespb.BuildEvent_Completed:
		{
			action.File = filesFromOutput(p.Completed.DirectoryOutput)
			return action
		}
	case *bespb.BuildEvent_TestResult:
		{
			action.File = filesFromOutput(p.TestResult.TestActionOutput)
			return action
		}
	}
	return nil
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
	switch p := event.Payload.(type) {
	case *bespb.BuildEvent_Configured:
		{
			ruleType := strings.Replace(p.Configured.TargetKind, " rule", "", -1)
			language := ""
			if components := strings.Split(p.Configured.TargetKind, "_"); len(components) > 1 {
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
				Tag:      p.Configured.Tag,
			}
		}
	case *bespb.BuildEvent_Completed:
		{
			target := tm[event.GetId().GetTargetCompleted().GetLabel()]
			target.Status = cmnpb.Status_BUILT
		}
	case *bespb.BuildEvent_TestSummary:
		{
			target := tm[event.GetId().GetTestSummary().GetLabel()]
			target.Status = TestStatusToStatus(p.TestSummary.OverallStatus)
			target.Timing = TestTimingFromSummary(p.TestSummary)
		}
	}
}

func TestTimingFromSummary(testSummary *bespb.TestSummary) *cmnpb.Timing {
	startTime := timeutil.GetTimeWithFallback(testSummary.FirstStartTime, testSummary.FirstStartTimeMillis)
	duration := timeutil.GetDurationWithFallback(testSummary.TotalRunDuration, testSummary.TotalRunDurationMillis)
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
