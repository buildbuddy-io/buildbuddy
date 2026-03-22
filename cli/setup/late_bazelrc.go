package setup

import (
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/options"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/parsed"
)

type importantBazelrcOption struct {
	label      string
	optionName string
	headerKey  string
}

type optionValue struct {
	present bool
	value   string
}

var importantLateBazelrcOptions = []importantBazelrcOption{
	{label: "bes_backend", optionName: "bes_backend"},
	{label: "remote_cache", optionName: "remote_cache"},
	{label: "remote_executor", optionName: "remote_executor"},
	{label: "remote_instance_name", optionName: "remote_instance_name"},
	{label: "target_pattern_file", optionName: "target_pattern_file"},
	{label: "query_file", optionName: "query_file"},
	{label: "sync", optionName: "sync"},
	{label: "invocation_id", optionName: "invocation_id"},
	{label: "remote_header=x-buildbuddy-api-key", headerKey: "x-buildbuddy-api-key"},
}

func warnOnImportantLateBazelrcChanges(beforeArgs, afterArgs []string) {
	lateBazelrcs, changedFlags, err := importantLateBazelrcChanges(beforeArgs, afterArgs)
	if len(lateBazelrcs) == 0 {
		return
	}
	if err != nil {
		log.Warnf(
			"A pre_bazel plugin added late --bazelrc file(s) (%s). BuildBuddy may not honor rc-derived settings before running Bazel.",
			strings.Join(lateBazelrcs, ", "),
		)
		log.Debugf("Failed to inspect late bazelrc changes: %s", err)
		return
	}
	if len(changedFlags) == 0 {
		return
	}
	log.Warnf(
		"A pre_bazel plugin added late --bazelrc file(s) (%s) that change BuildBuddy-managed flags (%s). Those rc-derived values are not applied by the CLI before Bazel starts.",
		strings.Join(lateBazelrcs, ", "),
		strings.Join(changedFlags, ", "),
	)
}

func importantLateBazelrcChanges(beforeArgs, afterArgs []string) ([]string, []string, error) {
	beforeParsed, err := parser.ParseArgs(beforeArgs)
	if err != nil {
		return nil, nil, err
	}
	afterParsed, err := parser.ParseArgs(afterArgs)
	if err != nil {
		return nil, nil, err
	}

	lateBazelrcs, err := lateEffectiveBazelrcs(beforeParsed, afterParsed)
	if err != nil || len(lateBazelrcs) == 0 {
		return lateBazelrcs, nil, err
	}

	resolvedAfter, err := parser.ResolveArgs(afterParsed)
	if err != nil {
		return lateBazelrcs, nil, err
	}

	beforeState, err := collectImportantArgState(beforeParsed)
	if err != nil {
		return lateBazelrcs, nil, err
	}
	afterState, err := collectImportantArgState(resolvedAfter)
	if err != nil {
		return lateBazelrcs, nil, err
	}

	var changed []string
	for _, opt := range importantLateBazelrcOptions {
		if beforeState[opt.label] != afterState[opt.label] {
			changed = append(changed, opt.label)
		}
	}
	return lateBazelrcs, changed, nil
}

func lateEffectiveBazelrcs(beforeParsed, afterParsed *parsed.OrderedArgs) ([]string, error) {
	ignoreAll, err := options.AccumulateValues[*parsed.IndexedOption](
		false,
		afterParsed.GetStartupOptionsByName("ignore_all_rc_files"),
	)
	if err != nil {
		return nil, err
	}
	if ignoreAll {
		return nil, nil
	}

	beforeCounts := make(map[string]int, len(beforeParsed.GetStartupOptionsByName("bazelrc")))
	for _, opt := range beforeParsed.GetStartupOptionsByName("bazelrc") {
		beforeCounts[opt.GetValue()]++
	}

	var late []string
	ignoreRemainingExplicitBazelrcs := false
	for _, opt := range afterParsed.GetStartupOptionsByName("bazelrc") {
		value := opt.GetValue()
		if ignoreRemainingExplicitBazelrcs {
			continue
		}
		if beforeCounts[value] > 0 {
			beforeCounts[value]--
			if value == "/dev/null" {
				ignoreRemainingExplicitBazelrcs = true
			}
			continue
		}
		if value == "/dev/null" {
			ignoreRemainingExplicitBazelrcs = true
			continue
		}
		late = append(late, value)
	}
	return late, nil
}

func collectImportantArgState(args *parsed.OrderedArgs) (map[string]optionValue, error) {
	state := make(map[string]optionValue, len(importantLateBazelrcOptions))
	for _, opt := range importantLateBazelrcOptions {
		if opt.headerKey != "" {
			if value := parser.GetRemoteHeaderVal(args, opt.headerKey); value != "" {
				state[opt.label] = optionValue{present: true, value: value}
			}
			continue
		}
		value, present, err := getBazelCommandOptionValue(args, opt.optionName)
		if err != nil {
			return nil, err
		}
		if present {
			state[opt.label] = optionValue{present: true, value: value}
		}
	}
	return state, nil
}

func getBazelCommandOptionValue(args *parsed.OrderedArgs, optionName string) (string, bool, error) {
	opts := args.GetCommandOptionsByName(optionName)
	if len(opts) == 0 {
		return "", false, nil
	}
	value, err := options.AccumulateValues[*parsed.IndexedOption]("", opts)
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}
