package explain

import (
	"testing"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	clpb "github.com/buildbuddy-io/buildbuddy/proto/command_line"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

func TestFindTimingProfileLog(t *testing.T) {
	tests := []struct {
		name        string
		version     string
		profilePath string
		logs        []*bespb.File
		wantName    string
	}{
		{
			name:     "default profile",
			version:  "7.6.0",
			logs:     []*bespb.File{{Name: "command.profile.gz", Uri: "bytestream://profile"}},
			wantName: "command.profile.gz",
		},
		{
			name:        "explicit profile path",
			version:     "7.6.0",
			profilePath: "/tmp/custom.profile.gz",
			logs: []*bespb.File{
				{Name: "command.profile.wrong", Uri: "bytestream://wrong"},
				{Name: "custom.profile.gz", Uri: "bytestream://profile"},
			},
			wantName: "custom.profile.gz",
		},
		{
			name:        "windows profile path",
			version:     "7.6.0",
			profilePath: `C:\tmp\custom.profile.gz`,
			logs:        []*bespb.File{{Name: "custom.profile.gz", Uri: "bytestream://profile"}},
			wantName:    "custom.profile.gz",
		},
		{
			name:     "bazel 8 generated profile name",
			version:  "8.0.0rc1",
			logs:     []*bespb.File{{Name: "command.profile.123.gz", Uri: "bytestream://profile"}},
			wantName: "command.profile.123.gz",
		},
		{
			name:     "does not guess for pre-bazel 8",
			version:  "7.6.0",
			logs:     []*bespb.File{{Name: "command.profile.wrong", Uri: "bytestream://wrong"}},
			wantName: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inv := timingProfileInvocation(test.version, test.profilePath, test.logs)
			got := findTimingProfileLog(inv)
			if got.GetName() != test.wantName {
				t.Fatalf("findTimingProfileLog() selected %q, want %q", got.GetName(), test.wantName)
			}
		})
	}
}

func timingProfileInvocation(version, profilePath string, logs []*bespb.File) *inpb.Invocation {
	inv := &inpb.Invocation{
		Event: []*inpb.InvocationEvent{
			{BuildEvent: &bespb.BuildEvent{Payload: &bespb.BuildEvent_Started{Started: &bespb.BuildStarted{BuildToolVersion: version}}}},
			{BuildEvent: &bespb.BuildEvent{Payload: &bespb.BuildEvent_BuildToolLogs{BuildToolLogs: &bespb.BuildToolLogs{Log: logs}}}},
		},
	}
	if profilePath != "" {
		inv.StructuredCommandLine = []*clpb.CommandLine{
			{
				CommandLineLabel: "canonical",
				Sections: []*clpb.CommandLineSection{
					{
						SectionLabel: "command options",
						SectionType: &clpb.CommandLineSection_OptionList{OptionList: &clpb.OptionList{Option: []*clpb.Option{
							{OptionName: "profile", OptionValue: profilePath},
						}}},
					},
				},
			},
		}
	}
	return inv
}
