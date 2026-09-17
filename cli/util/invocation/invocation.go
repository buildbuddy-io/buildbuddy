package invocation

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/log"

	clpb "github.com/buildbuddy-io/buildbuddy/proto/command_line"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	optpb "github.com/buildbuddy-io/buildbuddy/proto/options"
)

const (
	// explicitCommandLineMetadataKey is the build metadata key holding the command
	// line as the user typed it. It is set by the bb CLI and the ci runner.
	explicitCommandLineMetadataKey = "EXPLICIT_COMMAND_LINE"

	// commandLineLabelOriginal is the BES label for the command line as the user typed it, before
	// .bazelrc and --config expansion.
	commandLineLabelOriginal = "original"

	// sectionCommandOptions is the label of the command line section holding the
	// flags passed after the bazel command.
	// For example, with `bazel test --config=ci //...`, the command options section
	// would hold `--config=ci`.
	//
	// Startup options are in a separate section.
	sectionCommandOptions = "command options"

	// sectionResidual is the label of the command line section holding the
	// arguments that are not flags, i.e. the target patterns.
	sectionResidual = "residual"

	// redactedPlaceholder is the marker the BuildBuddy server substitutes for
	// secrets before storing build metadata. Keep in sync with
	// redact.redactedPlaceholder in server/util/redact.
	redactedPlaceholder = "<REDACTED>"
)

// ExplicitCommandLine returns the Bazel command line that produced the given
// invocation, as the user typed it, as an argv with the executable name
// removed.
func ExplicitCommandLine(inv *inpb.Invocation) ([]string, error) {
	// First try fetching it from build metadata. This is only set by the bb CLI and the CI runner.
	args, err := explicitCommandLineFromMetadata(inv)
	if err != nil {
		return nil, err
	}

	// Fallback to fetching it from the structed command line BES event.
	if args == nil {
		args = explicitCommandLineFromEvents(inv)
	}
	if args == nil {
		return nil, fmt.Errorf(
			"invocation %s records no explicit command line", inv.GetInvocationId())
	}

	args = stripRedacted(stripExecutable(args))
	if len(args) == 0 {
		return nil, fmt.Errorf(
			"invocation %s records no bazel command", inv.GetInvocationId())
	}
	return args, nil
}

// explicitCommandLineFromMetadata reads the argv recorded by `bb` or the CI
// runner. It returns nil (with no error) when the metadata is absent, which is
// the normal case for bazel invocations not run by the CLI.
func explicitCommandLineFromMetadata(inv *inpb.Invocation) ([]string, error) {
	raw, ok := buildMetadata(inv, explicitCommandLineMetadataKey)
	if !ok {
		return nil, nil
	}
	var args []string
	if err := json.Unmarshal([]byte(raw), &args); err != nil {
		return nil, fmt.Errorf("parse %s build metadata of invocation %s: %w",
			explicitCommandLineMetadataKey, inv.GetInvocationId(), err)
	}
	return args, nil
}

// explicitCommandLineFromEvents reassembles the command line from the BES.
//
// Startup options are deliberately left out, matching the UI: they are
// machine-specific (--output_base and friends) and rarely safe to replay
// somewhere else.
func explicitCommandLineFromEvents(inv *inpb.Invocation) []string {
	// The command (i.e. `test`, `run`, `build`) comes from the Started event.
	var command string
	for _, event := range inv.GetEvent() {
		if started := event.GetBuildEvent().GetStarted(); started != nil {
			command = started.GetCommand()
			break
		}
	}
	// Without the command it is impossible to reconstruct a valid Bazel
	// command, so bail out early.
	if command == "" {
		return nil
	}

	options := optionsInSection(inv, sectionCommandOptions)
	targetPatterns := chunksInSection(inv, sectionResidual)

	args := append([]string{command}, options...)
	// A "--" is only needed to protect negative target patterns.
	if slices.ContainsFunc(targetPatterns, func(t string) bool { return strings.HasPrefix(t, "-") }) {
		args = append(args, "--")
	}
	return append(args, targetPatterns...)
}

// originalCommandLine returns the "original" structured command line, i.e. the
// command line as the user typed it, before .bazelrc and --config expansion.
func originalCommandLine(inv *inpb.Invocation) *clpb.CommandLine {
	for _, commandLine := range inv.GetStructuredCommandLine() {
		if commandLine.GetCommandLineLabel() == commandLineLabelOriginal {
			return commandLine
		}
	}
	return nil
}

// chunksInSection returns the target patterns recorded in the given section of the
// command line event.
func chunksInSection(inv *inpb.Invocation, sectionLabel string) []string {
	var out []string
	for _, section := range originalCommandLine(inv).GetSections() {
		if section.GetSectionLabel() == sectionLabel {
			out = append(out, section.GetChunkList().GetChunk()...)
		}
	}
	return out
}

// optionsInSection returns the flags recorded in the given section of the
// structured command line event.
func optionsInSection(inv *inpb.Invocation, sectionLabel string) []string {
	var forms []string
	for _, section := range originalCommandLine(inv).GetSections() {
		if section.GetSectionLabel() != sectionLabel {
			continue
		}
		for _, option := range section.GetOptionList().GetOption() {
			form := option.GetCombinedForm()
			// Options tagged HIDDEN are skipped. These are specific to the machine that ran the build,
			// and shouldn't be re-applied when reproducing the build.
			if form == "" || slices.Contains(option.GetMetadataTags(), optpb.OptionMetadataTag_HIDDEN) {
				continue
			}
			forms = append(forms, form)
		}
	}
	return forms
}

// stripRedacted drops arguments whose values were redacted.
func stripRedacted(args []string) []string {
	kept := make([]string, 0, len(args))
	var dropped []string
	for _, arg := range args {
		if strings.Contains(arg, redactedPlaceholder) {
			dropped = append(dropped, arg)
			continue
		}
		kept = append(kept, arg)
	}
	if len(dropped) > 0 {
		log.Debugf(
			"Omitting %d redacted argument(s) from the recorded command line; the reproduction may not match the original run: %s",
			len(dropped), strings.Join(dropped, " "))
	}
	return kept
}

// stripExecutable drops a leading executable name if one is present.
func stripExecutable(args []string) []string {
	if len(args) > 0 {
		switch args[0] {
		case "bazel", "bazelisk", "bb":
			return args[1:]
		}
	}
	return args
}

// buildMetadata returns the value of a BuildMetadata key.
func buildMetadata(inv *inpb.Invocation, key string) (string, bool) {
	for _, event := range inv.GetEvent() {
		metadata := event.GetBuildEvent().GetBuildMetadata().GetMetadata()
		if value, ok := metadata[key]; ok {
			return value, true
		}
	}
	return "", false
}
