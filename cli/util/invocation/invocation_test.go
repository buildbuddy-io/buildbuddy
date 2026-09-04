package invocation

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	clpb "github.com/buildbuddy-io/buildbuddy/proto/command_line"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	optpb "github.com/buildbuddy-io/buildbuddy/proto/options"
)

// invocationWithMetadata builds an invocation carrying a BuildMetadata event,
// plus an unrelated event.
func invocationWithMetadata(metadata map[string]string) *inpb.Invocation {
	return &inpb.Invocation{
		InvocationId: "0f8fad5b-d9cb-469f-a165-70867728950e",
		Event: []*inpb.InvocationEvent{
			{BuildEvent: &bespb.BuildEvent{
				Payload: &bespb.BuildEvent_Progress{Progress: &bespb.Progress{}},
			}},
			{BuildEvent: &bespb.BuildEvent{
				Payload: &bespb.BuildEvent_BuildMetadata{
					BuildMetadata: &bespb.BuildMetadata{Metadata: metadata},
				},
			}},
		},
	}
}

func TestExplicitCommandLine_FromMetadata(t *testing.T) {
	for _, executable := range []string{"", "bb", "bazel", "bazelisk"} {
		commandLine := []string{"--nosystem_rc", "test", "--config=ci", "--nocache_test_results", "//foo:bar_test"}
		if executable != "" {
			commandLine = append([]string{executable}, commandLine...)
		}
		marshalled, err := json.Marshal(commandLine)
		require.NoError(t, err)
		inv := invocationWithMetadata(map[string]string{
			"EXPLICIT_COMMAND_LINE": string(marshalled),
			"COMMIT_SHA":            "abc123",
		})

		args, err := ExplicitCommandLine(inv)
		require.NoError(t, err)
		assert.Equal(t, []string{"--nosystem_rc", "test", "--config=ci", "--nocache_test_results", "//foo:bar_test"}, args)
	}
}

func TestExplicitCommandLine_ExecutableOnly(t *testing.T) {
	inv := invocationWithMetadata(map[string]string{"EXPLICIT_COMMAND_LINE": `["bb"]`})

	_, err := ExplicitCommandLine(inv)
	require.Error(t, err)
}

func TestExplicitCommandLine_PreservesTokens(t *testing.T) {
	inv := invocationWithMetadata(map[string]string{
		"EXPLICIT_COMMAND_LINE": `["aquery","--config=remote","--","mnemonic(\"CppCompile\", deps(//foo:bar_test))"]`,
	})

	args, err := ExplicitCommandLine(inv)
	require.NoError(t, err)
	assert.Equal(t, []string{"aquery", "--config=remote", "--", `mnemonic("CppCompile", deps(//foo:bar_test))`}, args)
}

func TestExplicitCommandLine_DropsRedactedArgs(t *testing.T) {
	inv := invocationWithMetadata(map[string]string{
		"EXPLICIT_COMMAND_LINE": `["test","--remote_header=<REDACTED>","--config=ci","//foo:bar_test"]`,
	})

	args, err := ExplicitCommandLine(inv)
	require.NoError(t, err)
	assert.Equal(t, []string{"test", "--config=ci", "//foo:bar_test"}, args)
}

// structuredCommandLineInvocation builds an invocation whose command line is
// recorded in the structured command line event.
func structuredCommandLineInvocation(sections ...*clpb.CommandLineSection) *inpb.Invocation {
	return &inpb.Invocation{
		InvocationId: "0f8fad5b-d9cb-469f-a165-70867728950e",
		// The command comes from the Started event.
		Event: []*inpb.InvocationEvent{
			{BuildEvent: &bespb.BuildEvent{
				Payload: &bespb.BuildEvent_Started{
					Started: &bespb.BuildStarted{Command: "test"},
				},
			}},
		},
		StructuredCommandLine: []*clpb.CommandLine{
			// The canonical command line is used as a decoy for tests.
			// It records the command after bazelrc expansion, and should never
			// be returned as the explicit command line.
			{
				CommandLineLabel: "canonical",
				Sections: []*clpb.CommandLineSection{
					chunkSection("command", "canonical-command-not-expected"),
					optionSection("command options", "--canonical_option_not_expected"),
					chunkSection("residual", "//canonical:target_not_expected"),
				},
			},
			{
				CommandLineLabel: "original",
				Sections:         sections,
			},
		},
	}
}

// hiddenOptionSection builds a "command options" section with HIDDEN metadata.
func hiddenOptionSection(label string, forms ...string) *clpb.CommandLineSection {
	section := optionSection(label, forms...)
	for _, option := range section.GetOptionList().GetOption() {
		option.MetadataTags = []optpb.OptionMetadataTag{optpb.OptionMetadataTag_HIDDEN}
	}
	return section
}

func chunkSection(label string, chunks ...string) *clpb.CommandLineSection {
	return &clpb.CommandLineSection{
		SectionLabel: label,
		SectionType:  &clpb.CommandLineSection_ChunkList{ChunkList: &clpb.ChunkList{Chunk: chunks}},
	}
}

func optionSection(label string, forms ...string) *clpb.CommandLineSection {
	options := make([]*clpb.Option, 0, len(forms))
	for _, f := range forms {
		options = append(options, &clpb.Option{CombinedForm: f})
	}
	return &clpb.CommandLineSection{
		SectionLabel: label,
		SectionType:  &clpb.CommandLineSection_OptionList{OptionList: &clpb.OptionList{Option: options}},
	}
}

func TestExplicitCommandLine_FromStructuredCommandLine(t *testing.T) {
	inv := structuredCommandLineInvocation(
		chunkSection("executable", "bazel"),
		optionSection("startup options", "--nosystem_rc"),
		chunkSection("command", "test"),
		optionSection("command options", "--config=ci", "--nocache_test_results"),
		chunkSection("residual", "//foo:bar_test"),
	)

	args, err := ExplicitCommandLine(inv)
	require.NoError(t, err)
	// Startup options are dropped, matching the UI: they are machine-specific.
	assert.Equal(t, []string{"test", "--config=ci", "--nocache_test_results", "//foo:bar_test"}, args)
}

func TestExplicitCommandLine_PrefersMetadataOverStructuredCommandLine(t *testing.T) {
	inv := structuredCommandLineInvocation(
		chunkSection("command", "test"),
		chunkSection("residual", "//from:structured"),
	)
	inv.Event = append(inv.Event, &inpb.InvocationEvent{
		BuildEvent: &bespb.BuildEvent{
			Payload: &bespb.BuildEvent_BuildMetadata{
				BuildMetadata: &bespb.BuildMetadata{Metadata: map[string]string{
					"EXPLICIT_COMMAND_LINE": `["test","//from:metadata"]`,
				}},
			},
		},
	})

	args, err := ExplicitCommandLine(inv)
	require.NoError(t, err)
	assert.Equal(t, []string{"test", "//from:metadata"}, args)
}

func TestExplicitCommandLine_StructuredCommandLine_NegativeTargets(t *testing.T) {
	// A "--" is required so bazel doesn't parse "-//foo:skip" as a flag.
	inv := structuredCommandLineInvocation(
		chunkSection("command", "test"),
		chunkSection("residual", "//...", "-//foo:skip"),
	)

	args, err := ExplicitCommandLine(inv)
	require.NoError(t, err)
	assert.Equal(t, []string{"test", "--", "//...", "-//foo:skip"}, args)
}

func TestExplicitCommandLine_StructuredCommandLine_DropsHiddenOptions(t *testing.T) {
	hidden := hiddenOptionSection("command options",
		"--rc_source=/home/runner/work/buildbuddy/buildbuddy/.bazelrc",
		"--client_cwd=/home/runner/work/buildbuddy/buildbuddy",
		"--binary_path=/home/runner/.cache/bazelisk/downloads/bin/bazel",
		"--startup_time=1988",
	)
	inv := structuredCommandLineInvocation(
		chunkSection("command", "test"),
		hidden,
		optionSection("command options", "--config=ci"),
		chunkSection("residual", "//foo:bar_test"),
	)

	args, err := ExplicitCommandLine(inv)
	require.NoError(t, err)
	assert.Equal(t, []string{"test", "--config=ci", "//foo:bar_test"}, args)
}
