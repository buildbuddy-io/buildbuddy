package timing_profile

import (
	"testing"

	"github.com/stretchr/testify/require"

	clpb "github.com/buildbuddy-io/buildbuddy/proto/command_line"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

func TestExtractEffectiveCommandLineSettings(t *testing.T) {
	inv := &inpb.Invocation{
		StructuredCommandLine: []*clpb.CommandLine{
			{
				CommandLineLabel: "original",
				Sections: []*clpb.CommandLineSection{
					optionSection(
						&clpb.Option{OptionName: "jobs", OptionValue: "50"},
						&clpb.Option{OptionName: "remote_download_outputs", OptionValue: "all"},
					),
				},
			},
			{
				CommandLineLabel: canonicalCommandLineLabel,
				Sections: []*clpb.CommandLineSection{
					optionSection(
						&clpb.Option{OptionName: "jobs", OptionValue: "50"},
						&clpb.Option{OptionName: "remote_executor", OptionValue: "grpcs://remote.buildbuddy.io"},
						&clpb.Option{OptionName: "remote_download_outputs", OptionValue: "minimal"},
						&clpb.Option{OptionName: "repository_cache", OptionValue: "/tmp/repository-cache"},
						// If the same option is passed multiple times, the last value is used.
						&clpb.Option{OptionName: "jobs", OptionValue: "100"},
					),
				},
			},
		},
	}

	settings := extractEffectiveCommandLineSettings(inv)
	require.True(t, settings.available)
	require.Equal(t, "100", settings.jobsValue)
	require.True(t, settings.remoteExecutionEnabled)
	require.True(t, settings.remoteDownloadMinimal)
	require.True(t, settings.repositoryCacheSet)
}

func optionSection(options ...*clpb.Option) *clpb.CommandLineSection {
	return &clpb.CommandLineSection{
		SectionType: &clpb.CommandLineSection_OptionList{
			OptionList: &clpb.OptionList{Option: options},
		},
	}
}
