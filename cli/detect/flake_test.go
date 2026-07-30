package detect

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clpb "github.com/buildbuddy-io/buildbuddy/proto/command_line"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

func TestReplayCommandFromInvocation(t *testing.T) {
	canonical := commandLine("canonical", []string{
		"--config=ci",
		"--remote_executor=grpcs://remote.buildbuddy.io",
		"--remote_header=x-buildbuddy-api-key=<REDACTED>",
		"--test_filter=OldFilter",
		"--runs_per_test=20",
		"--profile=/tmp/profile.gz",
	}, []string{"--", "//foo/...", "//bar:bar_test"})
	canonical.Sections = append([]*clpb.CommandLineSection{{
		SectionLabel: "startup options",
		SectionType: &clpb.CommandLineSection_OptionList{
			OptionList: &clpb.OptionList{Option: []*clpb.Option{
				{
					CombinedForm: "--install_md5=c3100451b619e4702a880475c90d93db",
					OptionName:   "install_md5",
				},
				{
					CombinedForm: "--max_idle_secs=0",
					OptionName:   "max_idle_secs",
				},
			}},
		},
	}}, canonical.Sections...)
	original := commandLine("original", []string{"--config=ci"}, []string{"//original/..."})
	original.Sections = append([]*clpb.CommandLineSection{{
		SectionLabel: "startup options",
		SectionType: &clpb.CommandLineSection_OptionList{
			OptionList: &clpb.OptionList{Option: []*clpb.Option{
				{
					CombinedForm: "--bazelrc=/ci/buildbuddy.bazelrc",
					OptionName:   "bazelrc",
				},
				{
					CombinedForm: "--host_jvm_args=-Xmx4g",
					OptionName:   "host_jvm_args",
				},
			}},
		},
	}}, original.Sections...)

	invocation := &inpb.Invocation{
		InvocationId: "12345678-1234-1234-1234-123456789012",
		Command:      "test",
		Pattern:      []string{"//fallback/..."},
		StructuredCommandLine: []*clpb.CommandLine{
			original,
			canonical,
		},
	}

	replay, skipped, err := replayCommandFromInvocation(invocation)
	require.NoError(t, err)

	assert.Equal(t, "test", replay.command)
	assert.Equal(t, []string{"--host_jvm_args=-Xmx4g", "--ignore_all_rc_files"}, replay.startupOptions)
	assert.Equal(t, []string{"--remote_executor=grpcs://remote.buildbuddy.io"}, replay.commandOptions)
	assert.Equal(t, []string{"//foo/...", "//bar:bar_test"}, replay.targets)
	assert.Equal(t, 1, skipped)
}

func TestReplayCommandFallsBackToOriginalAndInvocationPatterns(t *testing.T) {
	invocation := &inpb.Invocation{
		InvocationId: "12345678-1234-1234-1234-123456789012",
		Command:      "coverage",
		Pattern:      []string{"//foo:foo_test"},
		StructuredCommandLine: []*clpb.CommandLine{
			commandLine("original", []string{"--config=ci"}, nil),
		},
	}

	replay, _, err := replayCommandFromInvocation(invocation)
	require.NoError(t, err)
	assert.Equal(t, "coverage", replay.command)
	assert.Equal(t, []string{"--config=ci"}, replay.commandOptions)
	assert.Equal(t, invocation.Pattern, replay.targets)
}

func TestFlakeCheckerStopsWhenFilteredTestFails(t *testing.T) {
	runner := &fakeFlakeRunner{exitCodes: []int{0, 0, bazelTestFailureExitCode}}
	checker := &flakeChecker{runner: runner}

	detection, err := checker.Run(context.Background(), sampleReplayCommand(), "//foo:foo_test", "TestFlake", 5)
	require.NoError(t, err)
	require.NotNil(t, detection)

	assert.Equal(t, "the target and test filter", detection.strategy)
	assert.Equal(t, 3, detection.attempt)
	require.Len(t, runner.runs, 3)
	for _, run := range runner.runs {
		assert.Contains(t, run, "--nocache_test_results")
		assert.Contains(t, run, "--test_filter=TestFlake")
		assert.NotContains(t, run, "--runs_per_test=5")
		assert.Equal(t, "//foo:foo_test", run[len(run)-1])
	}
}

func TestFlakeCheckerBroadensStrategies(t *testing.T) {
	// Two passing filtered runs, two passing target-only runs, then a failing
	// full-command run.
	runner := &fakeFlakeRunner{exitCodes: []int{0, 0, 0, 0, bazelTestFailureExitCode}}
	checker := &flakeChecker{runner: runner}

	detection, err := checker.Run(context.Background(), sampleReplayCommand(), "//foo:foo_test", "TestFlake", 2)
	require.NoError(t, err)
	require.NotNil(t, detection)

	assert.Equal(t, "the full original command", detection.strategy)
	assert.Equal(t, 1, detection.attempt)
	require.Len(t, runner.runs, 5)

	targetOnlyRun := runner.runs[2]
	assert.NotContains(t, targetOnlyRun, "--test_filter=TestFlake")
	assert.Equal(t, "//foo:foo_test", targetOnlyRun[len(targetOnlyRun)-1])

	fullRun := runner.runs[4]
	assert.Contains(t, fullRun, "--runs_per_test=2")
	assert.Equal(t, []string{"//original/...", "//other:other_test"}, fullRun[len(fullRun)-2:])
}

func TestFlakeCheckerReturnsNilWhenNotReproduced(t *testing.T) {
	runner := &fakeFlakeRunner{exitCodes: []int{0, 0, 0, 0, 0}}
	checker := &flakeChecker{runner: runner}

	detection, err := checker.Run(context.Background(), sampleReplayCommand(), "//foo:foo_test", "TestFlake", 2)
	require.NoError(t, err)
	assert.Nil(t, detection)
	assert.Len(t, runner.runs, 5)
}

func TestFlakeCheckerTreatsNoTestsAsNotReproduced(t *testing.T) {
	runner := &fakeFlakeRunner{exitCodes: []int{
		bazelNoTestsExitCode,
		bazelNoTestsExitCode,
		bazelTestFailureExitCode,
	}}
	checker := &flakeChecker{runner: runner}

	detection, err := checker.Run(context.Background(), sampleReplayCommand(), "//foo:foo_test", "TestFlake", 2)
	require.NoError(t, err)
	require.NotNil(t, detection)
	assert.Equal(t, "the target without a test filter", detection.strategy)
}

func TestFlakeCheckerRejectsNonTestFailure(t *testing.T) {
	runner := &fakeFlakeRunner{exitCodes: []int{1}}
	checker := &flakeChecker{runner: runner}

	_, err := checker.Run(context.Background(), sampleReplayCommand(), "//foo:foo_test", "TestFlake", 2)
	require.ErrorContains(t, err, "exited with code 1")
}

func TestParseFlakeInvocationID(t *testing.T) {
	const invocationID = "12345678-1234-1234-1234-123456789012"
	for _, input := range []string{
		invocationID,
		"https://app.buildbuddy.io/invocation/" + invocationID,
		"https://app.buildbuddy.io/invocation/" + invocationID + "/",
	} {
		got, err := parseFlakeInvocationID(input)
		require.NoError(t, err)
		assert.Equal(t, invocationID, got)
	}

	_, err := parseFlakeInvocationID("not-an-invocation")
	require.Error(t, err)
}

func commandLine(label string, commandOptions, targets []string) *clpb.CommandLine {
	options := make([]*clpb.Option, 0, len(commandOptions))
	for _, option := range commandOptions {
		name := option
		if len(name) > 2 {
			name = name[2:]
		}
		for i, c := range name {
			if c == '=' {
				name = name[:i]
				break
			}
		}
		options = append(options, &clpb.Option{
			CombinedForm: option,
			OptionName:   name,
		})
	}
	return &clpb.CommandLine{
		CommandLineLabel: label,
		Sections: []*clpb.CommandLineSection{
			{
				SectionLabel: "command options",
				SectionType: &clpb.CommandLineSection_OptionList{
					OptionList: &clpb.OptionList{Option: options},
				},
			},
			{
				SectionLabel: "residual",
				SectionType: &clpb.CommandLineSection_ChunkList{
					ChunkList: &clpb.ChunkList{Chunk: targets},
				},
			},
		},
	}
}

func sampleReplayCommand() replayCommand {
	return replayCommand{
		startupOptions: []string{"--bazelrc=/tmp/bazelrc"},
		command:        "test",
		commandOptions: []string{"--config=ci"},
		targets:        []string{"//original/...", "//other:other_test"},
	}
}

type fakeFlakeRunner struct {
	exitCodes []int
	runs      [][]string
}

func (r *fakeFlakeRunner) Run(ctx context.Context, name string, args ...string) (int, error) {
	r.runs = append(r.runs, append([]string(nil), args...))
	if len(r.exitCodes) == 0 {
		return 0, nil
	}
	exitCode := r.exitCodes[0]
	r.exitCodes = r.exitCodes[1:]
	return exitCode, nil
}
