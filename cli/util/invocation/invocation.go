package invocation

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/log"

	clpb "github.com/buildbuddy-io/buildbuddy/proto/command_line"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

const (
	// explicitCommandLineMetadataKey is the build metadata key holding the command
	// line as the user typed it. It is set by the bb CLI and the ci runner.
	explicitCommandLineMetadataKey = "EXPLICIT_COMMAND_LINE"

	// commandLineLabelOriginal is the BES label for the command line as the user typed it, before
	// .bazelrc and --config expansion.
	commandLineLabelOriginal = "original"

	sectionCommand        = "command"
	sectionCommandOptions = "command options"
	sectionResidual       = "residual"

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

// explicitCommandLineFromEvents reassembles the command line from the
// "original" StructuredCommandLine event, which bazel emits for every
// invocation that streams to BES.
//
// Startup options are deliberately left out, matching the UI: they are
// machine-specific (--output_base and friends) and rarely safe to replay
// somewhere else.
func explicitCommandLineFromEvents(inv *inpb.Invocation) []string {
	var (
		commandLine *clpb.CommandLine
		optionsCmd  []string
		command     string
	)
	for _, event := range inv.GetEvent() {
		buildEvent := event.GetBuildEvent()
		if cl := buildEvent.GetStructuredCommandLine(); cl.GetCommandLineLabel() == commandLineLabelOriginal {
			commandLine = cl
		}
		if started := buildEvent.GetStarted(); started != nil && command == "" {
			command = started.GetCommand()
		}
		// Bazel also reports the explicit options as a flat list. The UI uses
		// this when the structured sections carry no options, so we do too.
		if options := buildEvent.GetOptionsParsed(); options != nil && optionsCmd == nil {
			optionsCmd = options.GetExplicitCmdLine()
		}
	}
	if commandLine == nil && command == "" {
		return nil
	}

	var options, residual []string
	for _, section := range commandLine.GetSections() {
		switch section.GetSectionLabel() {
		case sectionCommand:
			if chunks := section.GetChunkList().GetChunk(); len(chunks) > 0 && command == "" {
				command = chunks[0]
			}
		case sectionCommandOptions:
			options = append(options, combinedForms(section)...)
		case sectionResidual:
			residual = append(residual, section.GetChunkList().GetChunk()...)
		}
	}
	if len(options) == 0 {
		options = optionsCmd
	}
	if command == "" {
		return nil
	}

	args := append([]string{command}, options...)
	// A "--" is only needed to protect negative target patterns; adding it
	// unconditionally would turn residual args into executable args for
	// `bazel run`.
	if slices.ContainsFunc(residual, func(t string) bool { return strings.HasPrefix(t, "-") }) {
		args = append(args, "--")
	}
	return append(args, residual...)
}

// combinedForms returns each option in a section as it was set, e.g.
// "--nocache_test_results" rather than "--cache_test_results=false".
func combinedForms(section *clpb.CommandLineSection) []string {
	options := section.GetOptionList().GetOption()
	forms := make([]string, 0, len(options))
	for _, option := range options {
		if form := option.GetCombinedForm(); form != "" {
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
		log.Warnf(
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
