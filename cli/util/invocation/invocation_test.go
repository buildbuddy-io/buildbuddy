package invocation

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	clpb "github.com/buildbuddy-io/buildbuddy/proto/command_line"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
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

func structuredCommandLineInvocation(sections ...*clpb.CommandLineSection) *inpb.Invocation {
	return &inpb.Invocation{
		InvocationId: "0f8fad5b-d9cb-469f-a165-70867728950e",
		Event: []*inpb.InvocationEvent{
			{BuildEvent: &bespb.BuildEvent{
				Payload: &bespb.BuildEvent_StructuredCommandLine{
					StructuredCommandLine: &clpb.CommandLine{
						CommandLineLabel: "canonical",
						Sections: []*clpb.CommandLineSection{
							chunkSection("command", "test"),
							optionSection("command options", "--cache_test_results=false"),
						},
					},
				},
			}},
			{BuildEvent: &bespb.BuildEvent{
				Payload: &bespb.BuildEvent_StructuredCommandLine{
					StructuredCommandLine: &clpb.CommandLine{
						CommandLineLabel: "original",
						Sections:         sections,
					},
				},
			}},
		},
	}
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

func TestExplicitCommandLine_FromStructuredEvent(t *testing.T) {
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

func TestExplicitCommandLine_PrefersMetadataOverStructuredEvent(t *testing.T) {
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

func TestExplicitCommandLine_StructuredEvent_NegativeTargets(t *testing.T) {
	// A "--" is required so bazel doesn't parse "-//foo:skip" as a flag.
	inv := structuredCommandLineInvocation(
		chunkSection("command", "test"),
		chunkSection("residual", "//...", "-//foo:skip"),
	)

	args, err := ExplicitCommandLine(inv)
	require.NoError(t, err)
	assert.Equal(t, []string{"test", "--", "//...", "-//foo:skip"}, args)
}

func TestExplicitCommandLine_StructuredEvent_OptionsFromOptionsParsed(t *testing.T) {
	// Older bazel versions report explicit options only via OptionsParsed.
	inv := structuredCommandLineInvocation(
		chunkSection("command", "test"),
		chunkSection("residual", "//foo:bar_test"),
	)
	inv.Event = append(inv.Event, &inpb.InvocationEvent{
		BuildEvent: &bespb.BuildEvent{
			Payload: &bespb.BuildEvent_OptionsParsed{
				OptionsParsed: &bespb.OptionsParsed{ExplicitCmdLine: []string{"--config=ci"}},
			},
		},
	})

	args, err := ExplicitCommandLine(inv)
	require.NoError(t, err)
	assert.Equal(t, []string{"test", "--config=ci", "//foo:bar_test"}, args)
}
