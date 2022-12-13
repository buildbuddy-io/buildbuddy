package besutil

import (
	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
)

// VisitFiles calls the given func with each File message found in the given
// build event. The visit function may be called more than once for a single
// call of this function.
func VisitFiles(event *bespb.BuildEvent, visit func(...*bespb.File)) {
	switch p := event.Payload.(type) {
	case *bespb.BuildEvent_NamedSetOfFiles:
		visit(p.NamedSetOfFiles.GetFiles()...)
	case *bespb.BuildEvent_BuildToolLogs:
		visit(p.BuildToolLogs.GetLog()...)
	case *bespb.BuildEvent_TestResult:
		visit(p.TestResult.GetTestActionOutput()...)
	case *bespb.BuildEvent_TestSummary:
		visit(p.TestSummary.GetPassed()...)
		visit(p.TestSummary.GetFailed()...)
	case *bespb.BuildEvent_RunTargetAnalyzed:
		visit(p.RunTargetAnalyzed.GetRunfiles()...)
	case *bespb.BuildEvent_Action:
		visit(p.Action.GetStdout())
		visit(p.Action.GetStderr())
		visit(p.Action.GetPrimaryOutput())
		visit(p.Action.GetActionMetadataLogs()...)
	case *bespb.BuildEvent_Completed:
		visit(p.Completed.GetImportantOutput()...)
		visit(p.Completed.GetDirectoryOutput()...)
	}
}
