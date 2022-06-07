package common

import (
	"encoding/base64"
	"fmt"
	"net/url"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
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

// ActionsKey eturns a string key under which target level actions can be
// recorded in a metrics collector.
func ActionsKey(iid string) string {
	return "api/" + iid + "/cached_actions"
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
			action.Id.TargetId = EncodeID(event.GetId().GetTargetCompleted().GetLabel())
			action.Id.ConfigurationId = event.GetId().GetTargetCompleted().GetConfiguration().Id
			action.Id.ActionId = EncodeID("build")
			return action
		}
	case *bespb.BuildEvent_TestResult:
		{
			testResultID := event.GetId().GetTestResult()
			action.TargetLabel = event.GetId().GetTestResult().GetLabel()
			action.Id.TargetId = EncodeID(event.GetId().GetTestResult().GetLabel())
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
			action.File = filesFromOutput(p.Completed.ImportantOutput)
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
