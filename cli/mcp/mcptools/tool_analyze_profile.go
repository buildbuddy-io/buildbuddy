package mcptools

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	analyzeprofile "github.com/buildbuddy-io/buildbuddy/cli/analyze_profile"
	analyzeprofileschema "github.com/buildbuddy-io/buildbuddy/cli/mcp/mcptools/analyzeprofileschema"
	"github.com/buildbuddy-io/buildbuddy/cli/timing_profile"
	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	rapb "github.com/buildbuddy-io/buildbuddy/proto/remote_asset"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"github.com/buildbuddy-io/buildbuddy/server/util/trace_events"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"
	gstatus "google.golang.org/grpc/status"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
)

const (
	// Default number of top timing-profile entries returned when unset.
	defaultTopN = 10
	// Hard cap for top_n across timing-profile APIs.
	maxTopN = 50
	// Default max concurrent invocation metadata/profile preparation requests.
	defaultProfilePreparationParallelism = 8
	// Default max concurrent RE timing-profile analyses.
	defaultREParallelism = 100
	// Hard cap for RE timing-profile analysis parallelism.
	maxREParallelism = 1000
	// Default cap for serialized analyze_profile response size.
	defaultAnalyzeProfileMaxOutputBytes = 64 * 1024
	// Hard cap for max_output_bytes.
	maxAnalyzeProfileMaxOutputBytes = 10 * 1024 * 1024
)

var (
	//go:embed analyzeprofileschema/schema.go
	analyzeProfileSchemaSource string
)

type analyzeProfileResponseOptions struct {
	IncludeSummary                       bool
	IncludeSummaryTopSpans               bool
	IncludeSummaryTopCategories          bool
	IncludeSummaryTopThreads             bool
	IncludeSummaryCriticalPathComponents bool
	MaxOutputBytes                       int
}

func (s *Service) analyzeProfile(ctx context.Context, args map[string]any) (any, error) {
	topN, err := optionalPositiveInt(args, "top_n", defaultTopN, maxTopN)
	if err != nil {
		return nil, err
	}
	parallelism, err := optionalPositiveInt(args, "parallelism", defaultREParallelism, maxREParallelism)
	if err != nil {
		return nil, err
	}
	extraArgs, err := optionalStringSlice(args, "extra_args")
	if err != nil {
		return nil, err
	}
	hintsEnabled, err := optionalBool(args, "hints", false)
	if err != nil {
		return nil, err
	}
	includeSummary, err := optionalBool(args, "include_summary", false)
	if err != nil {
		return nil, err
	}
	includeSummaryTopSpans, err := optionalBool(args, "include_summary_top_spans", includeSummary)
	if err != nil {
		return nil, err
	}
	includeSummaryTopCategories, err := optionalBool(args, "include_summary_top_categories", includeSummary)
	if err != nil {
		return nil, err
	}
	includeSummaryTopThreads, err := optionalBool(args, "include_summary_top_threads", false)
	if err != nil {
		return nil, err
	}
	includeSummaryCriticalPathComponents, err := optionalBool(args, "include_summary_critical_path_components", false)
	if err != nil {
		return nil, err
	}
	maxOutputBytes, err := optionalPositiveInt(args, "max_output_bytes", defaultAnalyzeProfileMaxOutputBytes, maxAnalyzeProfileMaxOutputBytes)
	if err != nil {
		return nil, err
	}
	includeSummary = includeSummary || includeSummaryTopSpans || includeSummaryTopCategories || includeSummaryTopThreads || includeSummaryCriticalPathComponents
	responseOpts := analyzeProfileResponseOptions{
		IncludeSummary:                       includeSummary,
		IncludeSummaryTopSpans:               includeSummaryTopSpans,
		IncludeSummaryTopCategories:          includeSummaryTopCategories,
		IncludeSummaryTopThreads:             includeSummaryTopThreads,
		IncludeSummaryCriticalPathComponents: includeSummaryCriticalPathComponents,
		MaxOutputBytes:                       maxOutputBytes,
	}

	invocationRefs, err := invocationRefsFromArgs(args)
	if err != nil {
		return nil, err
	}
	invocationIDs := make([]string, 0, len(invocationRefs))
	seen := make(map[string]struct{}, len(invocationRefs))
	for _, ref := range invocationRefs {
		id, err := extractInvocationID(ref)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		invocationIDs = append(invocationIDs, id)
	}
	if len(invocationIDs) == 0 {
		return nil, fmt.Errorf("at least one invocation ID must be provided")
	}
	authCtx, err := s.authenticatedContext(ctx)
	if err != nil {
		return nil, err
	}

	env := real_environment.NewBatchEnv()
	env.SetByteStreamClient(s.bsClient)
	env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(s.conn))
	env.SetRemoteExecutionClient(repb.NewExecutionClient(s.conn))
	env.SetCapabilitiesClient(repb.NewCapabilitiesClient(s.conn))

	inputs := make([]*analyzeProfileInvocationInput, len(invocationIDs))
	results := make([]analyzeprofileschema.InvocationResult, len(invocationIDs))
	binaryDigestCache := newAnalyzeProfileBinaryDigestCache()

	// Phase 1: fetch invocation metadata and populate/fetch timing profile
	// digests. Keep this at a moderate fixed concurrency level.
	prepareEG, prepareCtx := errgroup.WithContext(authCtx)
	prepareEG.SetLimit(defaultProfilePreparationParallelism)
	for i, invocationID := range invocationIDs {
		prepareEG.Go(func() error {
			input, err := s.resolveTimingProfileInvocationInput(prepareCtx, invocationID)
			if err != nil {
				if isContextDoneErr(err) {
					return err
				}
				results[i] = timingProfileErrorResult(invocationID, err)
				return nil
			}
			if err := s.ensureTimingProfileResourceNameAvailable(prepareCtx, input); err != nil {
				if isContextDoneErr(err) {
					return err
				}
				results[i] = timingProfileErrorResult(invocationID, err)
				return nil
			}
			inputs[i] = input
			return nil
		})
	}
	if err := prepareEG.Wait(); err != nil {
		return nil, err
	}

	// Phase 2: run RE analysis with high concurrency.
	analysisEG, analysisCtx := errgroup.WithContext(authCtx)
	analysisEG.SetLimit(parallelism)
	for i, input := range inputs {
		if input == nil {
			continue
		}
		analysisEG.Go(func() error {
			result, err := s.analyzeProfileRemote(analysisCtx, env, input, topN, hintsEnabled, extraArgs, binaryDigestCache)
			if err != nil {
				if isContextDoneErr(err) {
					return err
				}
				results[i] = timingProfileErrorResult(input.InvocationID, err)
				return nil
			}
			if result == nil {
				results[i] = timingProfileErrorResult(input.InvocationID, fmt.Errorf("timing profile analysis returned no result"))
				return nil
			}
			results[i] = *result
			return nil
		})
	}
	if err := analysisEG.Wait(); err != nil {
		return nil, err
	}
	for i := range results {
		if results[i].InvocationID == "" {
			results[i] = timingProfileErrorResult(invocationIDs[i], fmt.Errorf("timing profile input was unavailable"))
		}
	}

	return aggregateTimingProfileResults(invocationIDs, topN, parallelism, results, responseOpts), nil
}

func (s *Service) analyzeProfileHelp(_ context.Context, _ map[string]any) (any, error) {
	return map[string]any{
		"help":                 analyzeprofile.Help(),
		"output_schema_source": analyzeProfileSchemaSource,
	}, nil
}

const (
	// Timeout used for remote-execution analysis actions.
	defaultActionTimeout = 45 * time.Second
	// Threshold where GC is considered likely impactful.
	highGCDurationPercentThreshold = 5.0
	// Threshold below which critical-path share suggests potential jobs/parallelism headroom.
	jobsCriticalPathDominancePercentThreshold = 75.0
	// Threshold below which observed average action parallelism relative to
	// configured --jobs suggests likely underutilization.
	jobsUtilizationPercentThreshold = 60.0
	// Threshold where queueing is considered likely a bottleneck.
	queueingBottleneckPercentThreshold = 20.0
	// Executable name expected inside RE input roots.
	analyzeProfileBinaryName = "bb-analyze-profile"
	// Env var overriding the analyzer binary source URL.
	analyzeProfileBinaryURLEnv = "BUILDBUDDY_ANALYZE_PROFILE_URL"
	// Env var overriding expected analyzer binary SHA256.
	analyzeProfileBinarySHA256Env = "BUILDBUDDY_ANALYZE_PROFILE_SHA256"
	// Default analyzer binary SHA256 when not overridden.
	defaultAnalyzeProfileBinarySHA256 = "809d29b6eb09e5681bb4e7c6211638c8f5d7b6714858f131595ec0755e12bb69"
	// Prefix used to construct default analyzer binary download URLs.
	defaultAnalyzeProfileBinaryURLPrefix = "https://storage.googleapis.com/buildbuddy-tools/binaries/bb-analyze-profile/bb-analyze-profile_linux-x86_64_musl_"
	// Remote Asset checksum qualifier name.
	checksumSRIQualifierName = "checksum.sri"
	// Remote Asset HTTP header qualifier prefix.
	httpHeaderQualifierPrefix = "http_header:"
	// Timeout for remote-asset fetches used to populate CAS.
	defaultAssetFetchTimeout = 2 * time.Minute
)

var (
	bazelTargetLabelPattern = regexp.MustCompile(`//[A-Za-z0-9._/@+=~-]+(?::[A-Za-z0-9._/@+=~-]+)?`)
)

func findTimingProfileFile(inv *inpb.Invocation) (*bespb.File, error) {
	buildToolLogs := latestBuildToolLogs(inv)
	if len(buildToolLogs) == 0 {
		return nil, fmt.Errorf("no BuildToolLogs were found for invocation %q", inv.GetInvocationId())
	}

	if major, ok := bazelVersionMajor(inv); ok && major >= 8 {
		// Match UI behavior in app/invocation/invocation_timing_card.tsx: for Bazel 8+,
		// pick the first command.profile.* entry with a URI.
		for _, file := range buildToolLogs {
			if strings.HasPrefix(file.GetName(), "command.profile.") && file.GetUri() != "" {
				return file, nil
			}
		}
		return nil, fmt.Errorf("no timing profile matching command.profile.* was found in BuildToolLogs for invocation %q", inv.GetInvocationId())
	}

	// Match UI behavior for Bazel <8: look up the canonical --profile value and
	// default to command.profile.gz when not set.
	profileName := canonicalProfileName(inv)
	for _, file := range buildToolLogs {
		if file.GetName() == profileName && file.GetUri() != "" {
			return file, nil
		}
	}
	return nil, fmt.Errorf("no timing profile named %q was found in BuildToolLogs for invocation %q", profileName, inv.GetInvocationId())
}

func latestBuildToolLogs(inv *inpb.Invocation) []*bespb.File {
	var logs []*bespb.File
	for _, invocationEvent := range inv.GetEvent() {
		buildEvent := invocationEvent.GetBuildEvent()
		if buildEvent == nil {
			continue
		}
		buildToolLogs := buildEvent.GetBuildToolLogs()
		if buildToolLogs == nil {
			continue
		}
		logs = buildToolLogs.GetLog()
	}
	return logs
}

func bazelVersionMajor(inv *inpb.Invocation) (int, bool) {
	var version string
	for _, invocationEvent := range inv.GetEvent() {
		buildEvent := invocationEvent.GetBuildEvent()
		if buildEvent == nil {
			continue
		}
		started := buildEvent.GetStarted()
		if started == nil {
			continue
		}
		version = strings.TrimSpace(started.GetBuildToolVersion())
	}
	if version == "" {
		return 0, false
	}

	segments := strings.Split(version, ".")
	if len(segments) < 2 {
		return 0, false
	}
	major, err := strconv.Atoi(segments[0])
	if err != nil {
		return 0, false
	}
	if _, err := strconv.Atoi(segments[1]); err != nil {
		return 0, false
	}
	return major, true
}

func canonicalProfileName(inv *inpb.Invocation) string {
	const defaultProfile = "command.profile.gz"

	// Keep this logic in sync with
	// InvocationTimingCardComponent.getProfileFile() in
	// app/invocation/invocation_timing_card.tsx.
	profilePath := defaultProfile
	for _, commandLine := range inv.GetStructuredCommandLine() {
		if commandLine.GetCommandLineLabel() != "canonical" {
			continue
		}
		for _, section := range commandLine.GetSections() {
			if section.GetSectionLabel() != "command options" {
				continue
			}
			for _, option := range section.GetOptionList().GetOption() {
				if option.GetOptionName() != "profile" {
					continue
				}
				candidate := strings.ReplaceAll(option.GetOptionValue(), "\\", "/")
				if candidate != "" {
					profilePath = candidate
				}
				break
			}
		}
		break
	}

	// Keep basename extraction behavior aligned with the UI implementation.
	if idx := strings.LastIndex(profilePath, "/"); idx >= 0 {
		profilePath = profilePath[idx+1:]
	}
	profilePath = strings.TrimSpace(profilePath)
	if profilePath == "" {
		return defaultProfile
	}
	return profilePath
}

func configuredJobsFromInvocation(inv *inpb.Invocation) (int64, string) {
	if inv == nil {
		return 0, "unknown"
	}
	if jobs, source, ok := jobsFromStructuredCommandLine(inv); ok {
		return jobs, source
	}
	if jobs, source, ok := jobsFromOptionsParsed(inv); ok {
		return jobs, source
	}
	if jobs, source, ok := jobsFromStartedOptionsDescription(inv); ok {
		return jobs, source
	}
	if cpuCount, source, ok := hostCPUCountFromStructuredCommandLine(inv); ok {
		return cpuCount, source
	}
	if cpuCount, source, ok := hostCPUCountFromOptionsParsed(inv); ok {
		return cpuCount, source
	}
	if cpuCount, source, ok := hostCPUCountFromStartedOptionsDescription(inv); ok {
		return cpuCount, source
	}
	return 0, "unknown"
}

func configuredRemoteExecutorFromInvocation(inv *inpb.Invocation) (string, string) {
	if inv == nil {
		return "", "unknown"
	}
	if remoteExecutor, source, ok := remoteExecutorFromStructuredCommandLine(inv); ok {
		return remoteExecutor, source
	}
	if remoteExecutor, source, ok := remoteExecutorFromOptionsParsed(inv); ok {
		return remoteExecutor, source
	}
	if remoteExecutor, source, ok := remoteExecutorFromStartedOptionsDescription(inv); ok {
		return remoteExecutor, source
	}
	return "", "unknown"
}

func remoteExecutorFromStructuredCommandLine(inv *inpb.Invocation) (string, string, bool) {
	const optionName = "remote_executor"
	for _, preferredLabel := range []string{"canonical", "original", "tool"} {
		if remoteExecutor, source, ok := optionStringValueFromStructuredCommandLine(inv, preferredLabel, optionName); ok {
			return remoteExecutor, joinSourceParts("structured_command_line", source), true
		}
	}
	if remoteExecutor, source, ok := optionStringValueFromStructuredCommandLine(inv, "", optionName); ok {
		return remoteExecutor, joinSourceParts("structured_command_line", source), true
	}
	return "", "", false
}

func optionStringValueFromStructuredCommandLine(inv *inpb.Invocation, commandLineLabel string, optionName string) (string, string, bool) {
	if inv == nil {
		return "", "", false
	}
	for _, commandLine := range inv.GetStructuredCommandLine() {
		label := commandLine.GetCommandLineLabel()
		if commandLineLabel != "" && label != commandLineLabel {
			continue
		}
		if commandLineLabel == "" && (label == "canonical" || label == "original" || label == "tool") {
			continue
		}
		for _, section := range commandLine.GetSections() {
			if section.GetSectionLabel() != "command options" {
				continue
			}
			for _, option := range section.GetOptionList().GetOption() {
				if option.GetOptionName() != optionName {
					continue
				}
				value := strings.TrimSpace(option.GetOptionValue())
				if value == "" {
					continue
				}
				sourceParts := make([]string, 0, 2)
				if label != "" {
					sourceParts = append(sourceParts, label)
				}
				if optionSource := strings.TrimSpace(option.GetSource()); optionSource != "" {
					sourceParts = append(sourceParts, optionSource)
				}
				return value, joinSourceParts(sourceParts...), true
			}
		}
	}
	return "", "", false
}

func remoteExecutorFromOptionsParsed(inv *inpb.Invocation) (string, string, bool) {
	if inv == nil {
		return "", "", false
	}
	for _, invocationEvent := range inv.GetEvent() {
		buildEvent := invocationEvent.GetBuildEvent()
		if buildEvent == nil {
			continue
		}
		optionsParsed := buildEvent.GetOptionsParsed()
		if optionsParsed == nil {
			continue
		}
		if remoteExecutor, ok := parseStringFlag(optionsParsed.GetExplicitCmdLine(), "remote_executor"); ok {
			return remoteExecutor, "options_parsed:explicit_cmd_line", true
		}
		if remoteExecutor, ok := parseStringFlag(optionsParsed.GetCmdLine(), "remote_executor"); ok {
			return remoteExecutor, "options_parsed:cmd_line", true
		}
	}
	return "", "", false
}

func remoteExecutorFromStartedOptionsDescription(inv *inpb.Invocation) (string, string, bool) {
	if inv == nil {
		return "", "", false
	}
	for _, invocationEvent := range inv.GetEvent() {
		buildEvent := invocationEvent.GetBuildEvent()
		if buildEvent == nil {
			continue
		}
		started := buildEvent.GetStarted()
		if started == nil {
			continue
		}
		if remoteExecutor, ok := parseStringFlag(strings.Fields(started.GetOptionsDescription()), "remote_executor"); ok {
			return remoteExecutor, "started:options_description", true
		}
	}
	return "", "", false
}

func jobsFromStructuredCommandLine(inv *inpb.Invocation) (int64, string, bool) {
	const optionName = "jobs"
	for _, preferredLabel := range []string{"canonical", "original", "tool"} {
		if jobs, source, ok := optionValueFromStructuredCommandLine(inv, preferredLabel, optionName); ok {
			return jobs, joinSourceParts("structured_command_line", source), true
		}
	}
	if jobs, source, ok := optionValueFromStructuredCommandLine(inv, "", optionName); ok {
		return jobs, joinSourceParts("structured_command_line", source), true
	}
	return 0, "", false
}

func optionValueFromStructuredCommandLine(inv *inpb.Invocation, commandLineLabel string, optionName string) (int64, string, bool) {
	if inv == nil {
		return 0, "", false
	}
	for _, commandLine := range inv.GetStructuredCommandLine() {
		label := commandLine.GetCommandLineLabel()
		if commandLineLabel != "" && label != commandLineLabel {
			continue
		}
		if commandLineLabel == "" && (label == "canonical" || label == "original" || label == "tool") {
			continue
		}
		for _, section := range commandLine.GetSections() {
			if section.GetSectionLabel() != "command options" {
				continue
			}
			for _, option := range section.GetOptionList().GetOption() {
				if option.GetOptionName() != optionName {
					continue
				}
				n, ok := parsePositiveInt64(option.GetOptionValue())
				if !ok {
					continue
				}
				sourceParts := make([]string, 0, 2)
				if label != "" {
					sourceParts = append(sourceParts, label)
				}
				if optionSource := strings.TrimSpace(option.GetSource()); optionSource != "" {
					sourceParts = append(sourceParts, optionSource)
				}
				return n, joinSourceParts(sourceParts...), true
			}
		}
	}
	return 0, "", false
}

func jobsFromOptionsParsed(inv *inpb.Invocation) (int64, string, bool) {
	if inv == nil {
		return 0, "", false
	}
	for _, invocationEvent := range inv.GetEvent() {
		buildEvent := invocationEvent.GetBuildEvent()
		if buildEvent == nil {
			continue
		}
		optionsParsed := buildEvent.GetOptionsParsed()
		if optionsParsed == nil {
			continue
		}
		if jobs, ok := parsePositiveIntFlag(optionsParsed.GetExplicitCmdLine(), "jobs"); ok {
			return jobs, "options_parsed:explicit_cmd_line", true
		}
		if jobs, ok := parsePositiveIntFlag(optionsParsed.GetCmdLine(), "jobs"); ok {
			return jobs, "options_parsed:cmd_line", true
		}
	}
	return 0, "", false
}

func jobsFromStartedOptionsDescription(inv *inpb.Invocation) (int64, string, bool) {
	if inv == nil {
		return 0, "", false
	}
	for _, invocationEvent := range inv.GetEvent() {
		buildEvent := invocationEvent.GetBuildEvent()
		if buildEvent == nil {
			continue
		}
		started := buildEvent.GetStarted()
		if started == nil {
			continue
		}
		if jobs, ok := parsePositiveIntFlag(strings.Fields(started.GetOptionsDescription()), "jobs"); ok {
			return jobs, "started:options_description", true
		}
	}
	return 0, "", false
}

func hostCPUCountFromStructuredCommandLine(inv *inpb.Invocation) (int64, string, bool) {
	const optionName = "local_cpu_resources"
	for _, preferredLabel := range []string{"canonical", "original", "tool"} {
		if cpuCount, source, ok := optionValueFromStructuredCommandLine(inv, preferredLabel, optionName); ok {
			return cpuCount, joinSourceParts("inferred_from_host_cpu_count", "structured_command_line", source), true
		}
	}
	if cpuCount, source, ok := optionValueFromStructuredCommandLine(inv, "", optionName); ok {
		return cpuCount, joinSourceParts("inferred_from_host_cpu_count", "structured_command_line", source), true
	}

	for _, commandLine := range inv.GetStructuredCommandLine() {
		label := commandLine.GetCommandLineLabel()
		for _, section := range commandLine.GetSections() {
			if section.GetSectionLabel() != "command options" {
				continue
			}
			for _, option := range section.GetOptionList().GetOption() {
				if option.GetOptionName() != "client_env" {
					continue
				}
				if cpuCount, ok := cpuCountFromClientEnvValue(option.GetOptionValue()); ok {
					source := "inferred_from_host_cpu_count:structured_command_line"
					if label != "" {
						source += ":" + label
					}
					return cpuCount, source, true
				}
			}
		}
	}
	return 0, "", false
}

func hostCPUCountFromOptionsParsed(inv *inpb.Invocation) (int64, string, bool) {
	if inv == nil {
		return 0, "", false
	}
	for _, invocationEvent := range inv.GetEvent() {
		buildEvent := invocationEvent.GetBuildEvent()
		if buildEvent == nil {
			continue
		}
		optionsParsed := buildEvent.GetOptionsParsed()
		if optionsParsed == nil {
			continue
		}
		if cpuCount, ok := parsePositiveIntFlag(optionsParsed.GetExplicitCmdLine(), "local_cpu_resources"); ok {
			return cpuCount, "inferred_from_host_cpu_count:options_parsed:explicit_cmd_line", true
		}
		if cpuCount, ok := parsePositiveIntFlag(optionsParsed.GetCmdLine(), "local_cpu_resources"); ok {
			return cpuCount, "inferred_from_host_cpu_count:options_parsed:cmd_line", true
		}
		for _, arg := range optionsParsed.GetExplicitCmdLine() {
			if cpuCount, ok := cpuCountFromClientEnvArg(arg); ok {
				return cpuCount, "inferred_from_host_cpu_count:options_parsed:explicit_cmd_line:client_env", true
			}
		}
		for _, arg := range optionsParsed.GetCmdLine() {
			if cpuCount, ok := cpuCountFromClientEnvArg(arg); ok {
				return cpuCount, "inferred_from_host_cpu_count:options_parsed:cmd_line:client_env", true
			}
		}
	}
	return 0, "", false
}

func hostCPUCountFromStartedOptionsDescription(inv *inpb.Invocation) (int64, string, bool) {
	if inv == nil {
		return 0, "", false
	}
	for _, invocationEvent := range inv.GetEvent() {
		buildEvent := invocationEvent.GetBuildEvent()
		if buildEvent == nil {
			continue
		}
		started := buildEvent.GetStarted()
		if started == nil {
			continue
		}
		args := strings.Fields(started.GetOptionsDescription())
		if cpuCount, ok := parsePositiveIntFlag(args, "local_cpu_resources"); ok {
			return cpuCount, "inferred_from_host_cpu_count:started:options_description", true
		}
		for _, arg := range args {
			if cpuCount, ok := cpuCountFromClientEnvArg(arg); ok {
				return cpuCount, "inferred_from_host_cpu_count:started:options_description:client_env", true
			}
		}
	}
	return 0, "", false
}

func parsePositiveIntFlag(args []string, flagName string) (int64, bool) {
	if len(args) == 0 || flagName == "" {
		return 0, false
	}
	longFlag := "--" + flagName
	prefix := longFlag + "="
	for i, arg := range args {
		arg = strings.TrimSpace(arg)
		if arg == longFlag {
			if i+1 >= len(args) {
				return 0, false
			}
			return parsePositiveInt64(args[i+1])
		}
		if strings.HasPrefix(arg, prefix) {
			return parsePositiveInt64(strings.TrimPrefix(arg, prefix))
		}
	}
	return 0, false
}

func parseStringFlag(args []string, flagName string) (string, bool) {
	if len(args) == 0 || flagName == "" {
		return "", false
	}
	longFlag := "--" + flagName
	prefix := longFlag + "="
	for i, arg := range args {
		arg = strings.TrimSpace(arg)
		if arg == longFlag {
			if i+1 >= len(args) {
				return "", false
			}
			value := strings.TrimSpace(args[i+1])
			if strings.HasPrefix(value, "--") || value == "" {
				return "", false
			}
			return value, true
		}
		if strings.HasPrefix(arg, prefix) {
			value := strings.TrimSpace(strings.TrimPrefix(arg, prefix))
			if value == "" {
				return "", false
			}
			return value, true
		}
	}
	return "", false
}

func parsePositiveInt64(raw string) (int64, bool) {
	raw = strings.Trim(strings.TrimSpace(raw), "'\"")
	if raw == "" {
		return 0, false
	}
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil && n > 0 {
		return n, true
	}
	if f, err := strconv.ParseFloat(raw, 64); err == nil && f > 0 {
		return int64(math.Round(f)), true
	}
	return 0, false
}

func cpuCountFromClientEnvArg(arg string) (int64, bool) {
	const prefix = "--client_env="
	if !strings.HasPrefix(arg, prefix) {
		return 0, false
	}
	return cpuCountFromClientEnvValue(strings.TrimPrefix(arg, prefix))
}

func cpuCountFromClientEnvValue(value string) (int64, bool) {
	key, rawValue, ok := strings.Cut(value, "=")
	if !ok {
		return 0, false
	}
	key = strings.ToUpper(strings.TrimSpace(key))
	switch key {
	case "NUMBER_OF_PROCESSORS", "NPROC", "HOST_CPUS", "BAZEL_HOST_CPUS":
		return parsePositiveInt64(rawValue)
	default:
		return 0, false
	}
}

func joinSourceParts(parts ...string) string {
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		filtered = append(filtered, part)
	}
	return strings.Join(filtered, ":")
}

type analyzeProfileInvocationInput struct {
	InvocationID         string
	ProfileFile          *bespb.File
	ProfileResourceName  *digest.CASResourceName
	ConfiguredJobs       int64
	ConfiguredJobsSource string
	RemoteExecutor       string
	RemoteExecutorSource string
}

type timingProfileResourceNameCacheEntry struct {
	ProfileResourceName  string
	ProfileName          string
	ProfileURI           string
	ConfiguredJobs       int64
	ConfiguredJobsSource string
	RemoteExecutor       string
	RemoteExecutorSource string
}

type analyzeProfileBinaryDigestCacheKey struct {
	InstanceName   string
	DigestFunction repb.DigestFunction_Value
}

type analyzeProfileBinaryDigestCacheEntry struct {
	once   sync.Once
	digest *repb.Digest
	err    error
}

type analyzeProfileBinaryDigestCache struct {
	mu      sync.Mutex
	entries map[analyzeProfileBinaryDigestCacheKey]*analyzeProfileBinaryDigestCacheEntry
}

func newAnalyzeProfileBinaryDigestCache() *analyzeProfileBinaryDigestCache {
	return &analyzeProfileBinaryDigestCache{
		entries: make(map[analyzeProfileBinaryDigestCacheKey]*analyzeProfileBinaryDigestCacheEntry),
	}
}

func (c *analyzeProfileBinaryDigestCache) getOrCreate(key analyzeProfileBinaryDigestCacheKey) *analyzeProfileBinaryDigestCacheEntry {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[key]
	if !ok {
		entry = &analyzeProfileBinaryDigestCacheEntry{}
		c.entries[key] = entry
	}
	return entry
}

func (s *Service) resolveTimingProfileInvocationInput(ctx context.Context, invocationID string) (*analyzeProfileInvocationInput, error) {
	if cachedInput, ok := s.getCachedTimingProfileInvocationInput(invocationID); ok {
		return cachedInput, nil
	}

	inv, err := s.fetchInvocation(ctx, invocationID)
	if err != nil {
		return nil, err
	}
	profileFile, err := findTimingProfileFile(inv)
	if err != nil {
		return nil, err
	}
	rn, err := digest.ParseBytestreamURI(profileFile.GetUri())
	if err != nil {
		return nil, err
	}
	configuredJobs, configuredJobsSource := configuredJobsFromInvocation(inv)
	remoteExecutor, remoteExecutorSource := configuredRemoteExecutorFromInvocation(inv)
	input := &analyzeProfileInvocationInput{
		InvocationID:         invocationID,
		ProfileFile:          profileFile,
		ProfileResourceName:  rn,
		ConfiguredJobs:       configuredJobs,
		ConfiguredJobsSource: configuredJobsSource,
		RemoteExecutor:       remoteExecutor,
		RemoteExecutorSource: remoteExecutorSource,
	}
	s.setCachedTimingProfileInvocationInput(invocationID, input)
	return input, nil
}

func (s *Service) getCachedTimingProfileInvocationInput(invocationID string) (*analyzeProfileInvocationInput, bool) {
	s.timingProfileResourceNameCacheMu.RLock()
	entry, ok := s.timingProfileResourceNameCache[invocationID]
	s.timingProfileResourceNameCacheMu.RUnlock()
	if !ok {
		return nil, false
	}
	rn, err := digest.ParseBytestreamURI(entry.ProfileResourceName)
	if err != nil {
		return nil, false
	}
	return &analyzeProfileInvocationInput{
		InvocationID: invocationID,
		ProfileFile: &bespb.File{
			Name: entry.ProfileName,
			File: &bespb.File_Uri{Uri: entry.ProfileURI},
		},
		ProfileResourceName:  rn,
		ConfiguredJobs:       entry.ConfiguredJobs,
		ConfiguredJobsSource: entry.ConfiguredJobsSource,
		RemoteExecutor:       entry.RemoteExecutor,
		RemoteExecutorSource: entry.RemoteExecutorSource,
	}, true
}

func (s *Service) setCachedTimingProfileInvocationInput(invocationID string, input *analyzeProfileInvocationInput) {
	if input == nil || input.ProfileResourceName == nil {
		return
	}
	entry := timingProfileResourceNameCacheEntry{
		ProfileResourceName:  input.ProfileResourceName.DownloadString(),
		ConfiguredJobs:       input.ConfiguredJobs,
		ConfiguredJobsSource: input.ConfiguredJobsSource,
		RemoteExecutor:       input.RemoteExecutor,
		RemoteExecutorSource: input.RemoteExecutorSource,
	}
	if input.ProfileFile != nil {
		entry.ProfileName = input.ProfileFile.GetName()
		entry.ProfileURI = input.ProfileFile.GetUri()
	}
	s.timingProfileResourceNameCacheMu.Lock()
	s.timingProfileResourceNameCache[invocationID] = entry
	s.timingProfileResourceNameCacheMu.Unlock()
}

func (s *Service) analyzeProfileRemote(
	ctx context.Context,
	env *real_environment.RealEnv,
	input *analyzeProfileInvocationInput,
	topN int,
	hintsEnabled bool,
	extraArgs []string,
	binaryDigestCache *analyzeProfileBinaryDigestCache,
) (*analyzeprofileschema.InvocationResult, error) {
	instanceName := input.ProfileResourceName.GetInstanceName()
	digestFunction := input.ProfileResourceName.GetDigestFunction()
	analyzerDigest, err := s.getOrFetchAnalyzeProfileBinaryDigest(ctx, instanceName, digestFunction, binaryDigestCache)
	if err != nil {
		return nil, err
	}

	inputRoot := &repb.Directory{
		Files: []*repb.FileNode{
			{
				Name:         analyzeProfileBinaryName,
				Digest:       analyzerDigest,
				IsExecutable: true,
			},
			{
				Name:   "timing_profile",
				Digest: input.ProfileResourceName.GetDigest(),
			},
		},
	}
	sort.Slice(inputRoot.GetFiles(), func(i, j int) bool {
		return inputRoot.GetFiles()[i].GetName() < inputRoot.GetFiles()[j].GetName()
	})
	inputRootDigest, err := cachetools.UploadProto(ctx, s.bsClient, instanceName, digestFunction, inputRoot)
	if err != nil {
		return nil, fmt.Errorf("upload RE input root for invocation %q: %w", input.InvocationID, err)
	}

	cmd := &repb.Command{
		Arguments: buildAnalyzeProfileCommandArgs(topN, extraArgs),
	}
	action := &repb.Action{
		InputRootDigest: inputRootDigest,
		DoNotCache:      true,
		Timeout:         durationpb.New(defaultActionTimeout),
	}
	arn, err := rexec.Prepare(ctx, env, instanceName, digestFunction, action, cmd, "")
	if err != nil {
		return nil, fmt.Errorf("prepare RE action for invocation %q: %w", input.InvocationID, err)
	}
	stream, err := rexec.Start(ctx, env, arn)
	if err != nil {
		return nil, fmt.Errorf("start RE action for invocation %q: %w", input.InvocationID, err)
	}
	defer stream.CloseSend()
	execResp, err := rexec.Wait(stream)
	if err != nil {
		return nil, fmt.Errorf("wait for RE action for invocation %q: %w", input.InvocationID, err)
	}
	if execResp.Err != nil {
		return nil, fmt.Errorf("remote execution status for invocation %q: %w", input.InvocationID, execResp.Err)
	}
	if execResp.ExecuteResponse == nil || execResp.ExecuteResponse.GetResult() == nil {
		return nil, fmt.Errorf("remote execution result for invocation %q was empty", input.InvocationID)
	}
	result, err := rexec.GetResult(ctx, env, instanceName, digestFunction, execResp.ExecuteResponse.GetResult())
	if err != nil {
		return nil, fmt.Errorf("download RE result for invocation %q: %w", input.InvocationID, err)
	}
	if result.Error != nil {
		return nil, fmt.Errorf("remote analysis for invocation %q: %w", input.InvocationID, result.Error)
	}
	if result.ExitCode != 0 {
		return nil, fmt.Errorf(
			"remote analysis for invocation %q exited with code %d: %s",
			input.InvocationID,
			result.ExitCode,
			trimOutputTail(result.Stderr),
		)
	}

	var parsed struct {
		ProfilePath   string                 `json:"profile_path"`
		ProfileIsGzip bool                   `json:"profile_is_gzip"`
		Summary       timing_profile.Summary `json:"summary"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(result.Stdout), &parsed); err != nil {
		return nil, fmt.Errorf(
			"parse remote JSON summary for invocation %q: %w; stdout=%q",
			input.InvocationID,
			err,
			trimOutputTail(result.Stdout),
		)
	}
	parsed.Summary.ConfiguredJobs = input.ConfiguredJobs
	parsed.Summary.ConfiguredJobsSource = input.ConfiguredJobsSource

	actionResourceName := ""
	if actionRN, err := digest.CASResourceNameFromProto(arn); err == nil {
		actionResourceName = actionRN.DownloadString()
	}
	insights := trace_events.SummarizeTimingProfileInsights(parsed.Summary, insightThresholds())
	diagnostics := trace_events.SummarizeTimingProfileDiagnostics(parsed.Summary)
	hints := analyzeProfileHints(input, parsed.Summary, topN, hintsEnabled)

	return &analyzeprofileschema.InvocationResult{
		InvocationID:  input.InvocationID,
		Status:        "ok",
		ProfileFile:   timingProfileFileSummary(input.ProfileFile, input.ProfileResourceName),
		ProfileIsGzip: parsed.ProfileIsGzip,
		Summary:       &parsed.Summary,
		Insights:      &insights,
		Diagnostics:   &diagnostics,
		Hints:         hints,
		RemoteExecution: &analyzeprofileschema.RemoteExecution{
			InstanceName:       instanceName,
			DigestFunction:     digestFunction.String(),
			ActionResourceName: actionResourceName,
			ExitCode:           result.ExitCode,
		},
	}, nil
}

func buildAnalyzeProfileCommandArgs(topN int, extraArgs []string) []string {
	args := make([]string, 0, len(extraArgs)+4)
	args = append(args, "./"+analyzeProfileBinaryName)
	args = append(args, extraArgs...)
	if !hasLongFlag(extraArgs, "profile") {
		args = append(args, "--profile=timing_profile")
	}
	if !hasLongFlag(extraArgs, "top_n") {
		args = append(args, fmt.Sprintf("--top_n=%d", topN))
	}
	return args
}

func hasLongFlag(args []string, flagName string) bool {
	prefix := "--" + flagName
	withEquals := prefix + "="
	for _, arg := range args {
		if arg == prefix || strings.HasPrefix(arg, withEquals) {
			return true
		}
	}
	return false
}

func analyzeProfileHints(
	input *analyzeProfileInvocationInput,
	summary timing_profile.Summary,
	topN int,
	hintsEnabled bool,
) []analyzeprofileschema.Hint {
	if !hintsEnabled || input == nil {
		return nil
	}
	if strings.TrimSpace(input.RemoteExecutor) == "" {
		return nil
	}

	limit := topN
	if limit <= 0 {
		limit = defaultTopN
	}
	if limit > 10 {
		limit = 10
	}

	labels := slowTargetLabelsFromSummary(summary, limit)
	if len(labels) == 0 {
		return []analyzeprofileschema.Hint{
			{
				Message: "This invocation is RBE-enabled (--remote_executor is set). Call get_executions with this invocation_id to inspect slow remote actions.",
				Tool:    "get_executions",
				GetExecutions: &analyzeprofileschema.GetExecutionsHintArguments{
					InvocationID: input.InvocationID,
				},
			},
		}
	}

	hints := make([]analyzeprofileschema.Hint, 0, len(labels))
	for _, targetLabel := range labels {
		hints = append(hints, analyzeprofileschema.Hint{
			Message: fmt.Sprintf("Target %q appears among the slowest critical-path work. Call get_executions for execution-level CPU/IO/network details.", targetLabel),
			Tool:    "get_executions",
			GetExecutions: &analyzeprofileschema.GetExecutionsHintArguments{
				InvocationID: input.InvocationID,
				TargetLabel:  targetLabel,
			},
		})
	}
	return hints
}

type targetDuration struct {
	TargetLabel  string
	DurationUsec int64
}

func slowTargetLabelsFromSummary(summary timing_profile.Summary, limit int) []string {
	if limit <= 0 {
		return nil
	}

	durationByLabel := make(map[string]int64)
	for _, component := range summary.CriticalPathComponents {
		for _, label := range extractBazelTargetLabels(component.Name) {
			durationByLabel[label] += max(0, component.DurationUsec)
		}
	}
	for _, span := range summary.TopSpansByDurationUsec {
		for _, label := range extractBazelTargetLabels(span.Name) {
			durationByLabel[label] += max(0, span.DurationUsec)
		}
	}

	if len(durationByLabel) == 0 {
		return nil
	}
	ordered := make([]targetDuration, 0, len(durationByLabel))
	for targetLabel, durationUsec := range durationByLabel {
		ordered = append(ordered, targetDuration{
			TargetLabel:  targetLabel,
			DurationUsec: durationUsec,
		})
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].DurationUsec == ordered[j].DurationUsec {
			return ordered[i].TargetLabel < ordered[j].TargetLabel
		}
		return ordered[i].DurationUsec > ordered[j].DurationUsec
	})
	if len(ordered) > limit {
		ordered = ordered[:limit]
	}
	labels := make([]string, 0, len(ordered))
	for _, candidate := range ordered {
		labels = append(labels, candidate.TargetLabel)
	}
	return labels
}

func extractBazelTargetLabels(text string) []string {
	if strings.TrimSpace(text) == "" {
		return nil
	}
	matches := bazelTargetLabelPattern.FindAllString(text, -1)
	if len(matches) == 0 {
		return nil
	}
	out := make([]string, 0, len(matches))
	seen := make(map[string]struct{}, len(matches))
	for _, match := range matches {
		match = strings.TrimSpace(match)
		if match == "" {
			continue
		}
		if _, ok := seen[match]; ok {
			continue
		}
		seen[match] = struct{}{}
		out = append(out, match)
	}
	return out
}

func (s *Service) ensureTimingProfileResourceNameAvailable(ctx context.Context, input *analyzeProfileInvocationInput) error {
	if input == nil || input.ProfileResourceName == nil || input.ProfileResourceName.GetDigest() == nil {
		return fmt.Errorf("timing profile digest is missing")
	}

	missing, err := s.isDigestMissingInCAS(ctx, input.ProfileResourceName)
	if err != nil {
		return fmt.Errorf("check timing profile digest availability for invocation %q: %w", input.InvocationID, err)
	}
	if !missing {
		return nil
	}

	if err := s.fetchTimingProfileViaRemoteAsset(ctx, input); err != nil {
		return fmt.Errorf("populate timing profile digest for invocation %q: %w", input.InvocationID, err)
	}

	missing, err = s.isDigestMissingInCAS(ctx, input.ProfileResourceName)
	if err != nil {
		return fmt.Errorf("recheck timing profile digest availability for invocation %q: %w", input.InvocationID, err)
	}
	if missing {
		return fmt.Errorf(
			"timing profile digest %q is still missing after remote fetch",
			input.ProfileResourceName.DownloadString(),
		)
	}
	return nil
}

func (s *Service) isDigestMissingInCAS(ctx context.Context, rn *digest.CASResourceName) (bool, error) {
	if rn == nil || rn.GetDigest() == nil {
		return false, fmt.Errorf("resource name digest is empty")
	}
	req := &repb.FindMissingBlobsRequest{
		InstanceName:   rn.GetInstanceName(),
		BlobDigests:    []*repb.Digest{rn.GetDigest()},
		DigestFunction: rn.GetDigestFunction(),
	}
	rsp, err := repb.NewContentAddressableStorageClient(s.conn).FindMissingBlobs(ctx, req)
	if err != nil {
		return false, err
	}
	return len(rsp.GetMissingBlobDigests()) > 0, nil
}

func (s *Service) fetchTimingProfileViaRemoteAsset(ctx context.Context, input *analyzeProfileInvocationInput) error {
	fetchURL, err := s.fileDownloadURL(input)
	if err != nil {
		return err
	}
	sri, err := checksumSRI(input.ProfileResourceName.GetDigest(), input.ProfileResourceName.GetDigestFunction())
	if err != nil {
		return err
	}
	apiKey, err := apiKeyFromContext(ctx)
	if err != nil {
		return err
	}

	req := &rapb.FetchBlobRequest{
		InstanceName:   input.ProfileResourceName.GetInstanceName(),
		Uris:           []string{fetchURL},
		DigestFunction: input.ProfileResourceName.GetDigestFunction(),
		Timeout:        durationpb.New(defaultAssetFetchTimeout),
		Qualifiers: []*rapb.Qualifier{
			{
				Name:  checksumSRIQualifierName,
				Value: sri,
			},
			{
				Name:  httpHeaderQualifierPrefix + apiKeyHeader,
				Value: apiKey,
			},
		},
	}
	resp, err := rapb.NewFetchClient(s.conn).FetchBlob(ctx, req)
	if err != nil {
		return err
	}
	if err := gstatus.ErrorProto(resp.GetStatus()); err != nil {
		return err
	}
	if err := validateFetchedDigest(resp, input.ProfileResourceName.GetDigest(), input.ProfileResourceName.GetDigestFunction()); err != nil {
		return err
	}
	return nil
}

func (s *Service) fileDownloadURL(input *analyzeProfileInvocationInput) (string, error) {
	if input == nil || input.ProfileFile == nil {
		return "", fmt.Errorf("timing profile file metadata is missing")
	}
	return s.fileDownloadURLForArtifact(input.InvocationID, input.ProfileFile.GetUri())
}

// checksumSRI encodes a digest as a "checksum.sri" qualifier value understood
// by Remote Asset APIs, in the format "<digest-function-lower>-<base64(hash)>".
//
// Example:
//   - digestFunction=SHA256, hash=<hex bytes>
//   - returns "sha256-<base64(raw-hash-bytes)>"
func checksumSRI(d *repb.Digest, digestFunction repb.DigestFunction_Value) (string, error) {
	if d == nil {
		return "", fmt.Errorf("digest is empty")
	}
	hash := strings.TrimSpace(d.GetHash())
	if hash == "" {
		return "", fmt.Errorf("digest hash is empty")
	}
	digestBytes, err := hex.DecodeString(hash)
	if err != nil {
		return "", fmt.Errorf("decode digest hash %q: %w", hash, err)
	}
	prefix := strings.ToLower(digestFunction.String())
	if prefix == "" || prefix == strings.ToLower(repb.DigestFunction_UNKNOWN.String()) {
		return "", fmt.Errorf("digest function is unknown")
	}
	return prefix + "-" + base64.StdEncoding.EncodeToString(digestBytes), nil
}

func apiKeyFromContext(ctx context.Context) (string, error) {
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		values := md.Get(apiKeyHeader)
		if len(values) > 0 {
			key := strings.TrimSpace(values[len(values)-1])
			if key != "" {
				return key, nil
			}
		}
	}
	return requiredAPIKey()
}

func validateFetchedDigest(resp *rapb.FetchBlobResponse, expectedDigest *repb.Digest, expectedDigestFunction repb.DigestFunction_Value) error {
	gotDigest := resp.GetBlobDigest()
	if gotDigest == nil {
		return fmt.Errorf("fetch blob response did not include a digest")
	}
	gotDigestFunction := resp.GetDigestFunction()
	if gotDigestFunction == repb.DigestFunction_UNKNOWN {
		gotDigestFunction = expectedDigestFunction
	}
	if gotDigestFunction != expectedDigestFunction {
		return fmt.Errorf("fetch blob digest function mismatch: got %s, expected %s", gotDigestFunction, expectedDigestFunction)
	}
	if gotDigest.GetHash() != expectedDigest.GetHash() {
		return fmt.Errorf("fetch blob digest hash mismatch: got %s, expected %s", gotDigest.GetHash(), expectedDigest.GetHash())
	}
	if expectedDigest.GetSizeBytes() > 0 && gotDigest.GetSizeBytes() != expectedDigest.GetSizeBytes() {
		return fmt.Errorf("fetch blob digest size mismatch: got %d, expected %d", gotDigest.GetSizeBytes(), expectedDigest.GetSizeBytes())
	}
	return nil
}

type analyzeProfileBinarySpec struct {
	URL    string
	SHA256 string
}

func (s *Service) getOrFetchAnalyzeProfileBinaryDigest(
	ctx context.Context,
	instanceName string,
	digestFunction repb.DigestFunction_Value,
	binaryDigestCache *analyzeProfileBinaryDigestCache,
) (*repb.Digest, error) {
	key := analyzeProfileBinaryDigestCacheKey{
		InstanceName:   instanceName,
		DigestFunction: digestFunction,
	}
	entry := binaryDigestCache.getOrCreate(key)

	entry.once.Do(func() {
		spec, err := resolveAnalyzeProfileBinarySpec()
		if err != nil {
			entry.err = err
			return
		}
		sri, err := checksumSRI(&repb.Digest{Hash: spec.SHA256}, repb.DigestFunction_SHA256)
		if err != nil {
			entry.err = fmt.Errorf("compute checksum qualifier for analyze profile binary: %w", err)
			return
		}
		req := &rapb.FetchBlobRequest{
			InstanceName:   instanceName,
			Uris:           []string{spec.URL},
			DigestFunction: digestFunction,
			Timeout:        durationpb.New(defaultAssetFetchTimeout),
			Qualifiers: []*rapb.Qualifier{
				{
					Name:  checksumSRIQualifierName,
					Value: sri,
				},
			},
		}
		resp, err := rapb.NewFetchClient(s.conn).FetchBlob(ctx, req)
		if err != nil {
			entry.err = fmt.Errorf("fetch analyze profile binary %q via remote asset API: %w", spec.URL, err)
			return
		}
		if err := gstatus.ErrorProto(resp.GetStatus()); err != nil {
			entry.err = fmt.Errorf("fetch analyze profile binary %q via remote asset API: %w", spec.URL, err)
			return
		}

		fetchedDigest := resp.GetBlobDigest()
		if fetchedDigest == nil {
			entry.err = fmt.Errorf("fetch analyze profile binary %q via remote asset API: missing digest in response", spec.URL)
			return
		}
		fetchedDigestFunction := resp.GetDigestFunction()
		if fetchedDigestFunction == repb.DigestFunction_UNKNOWN {
			fetchedDigestFunction = digestFunction
		}
		if fetchedDigestFunction != digestFunction {
			entry.err = fmt.Errorf("fetch analyze profile binary %q returned digest function %s, expected %s", spec.URL, fetchedDigestFunction, digestFunction)
			return
		}
		if digestFunction == repb.DigestFunction_SHA256 && fetchedDigest.GetHash() != spec.SHA256 {
			entry.err = fmt.Errorf("fetch analyze profile binary %q returned SHA256 %q, expected %q", spec.URL, fetchedDigest.GetHash(), spec.SHA256)
			return
		}

		entry.digest = fetchedDigest
	})
	if entry.err != nil {
		return nil, entry.err
	}
	return entry.digest, nil
}

func resolveAnalyzeProfileBinarySpec() (*analyzeProfileBinarySpec, error) {
	urlValue := strings.TrimSpace(os.Getenv(analyzeProfileBinaryURLEnv))
	hashValue := strings.TrimSpace(os.Getenv(analyzeProfileBinarySHA256Env))
	if hashValue == "" {
		if urlValue != "" {
			hashValue = inferSHA256FromAnalyzeProfileURL(urlValue)
		}
		if hashValue == "" {
			hashValue = defaultAnalyzeProfileBinarySHA256
		}
	}
	if urlValue == "" && hashValue != "" {
		urlValue = defaultAnalyzeProfileBinaryURLPrefix + hashValue
	}
	hashValue, err := normalizeSHA256(hashValue)
	if err != nil {
		return nil, fmt.Errorf("resolve analyze profile binary hash: %w", err)
	}
	if urlValue == "" {
		return nil, fmt.Errorf("missing analyze profile binary URL; set %s", analyzeProfileBinaryURLEnv)
	}
	if _, err := url.ParseRequestURI(urlValue); err != nil {
		return nil, fmt.Errorf("parse %s=%q: %w", analyzeProfileBinaryURLEnv, urlValue, err)
	}
	return &analyzeProfileBinarySpec{
		URL:    urlValue,
		SHA256: hashValue,
	}, nil
}

func inferSHA256FromAnalyzeProfileURL(rawURL string) string {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return ""
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	base := path.Base(parsed.Path)
	idx := strings.LastIndex(base, "_")
	if idx < 0 || idx+1 >= len(base) {
		return ""
	}
	candidate := base[idx+1:]
	hashValue, err := normalizeSHA256(candidate)
	if err != nil {
		return ""
	}
	return hashValue
}

func normalizeSHA256(value string) (string, error) {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return "", fmt.Errorf(
			"sha256 is required; set %s or update defaultAnalyzeProfileBinarySHA256",
			analyzeProfileBinarySHA256Env,
		)
	}
	if len(value) != sha256.Size*2 {
		return "", fmt.Errorf("expected 64 hex chars, got %d", len(value))
	}
	if _, err := hex.DecodeString(value); err != nil {
		return "", fmt.Errorf("decode hex: %w", err)
	}
	return value, nil
}

func aggregateTimingProfileResults(
	invocationIDs []string,
	topN int,
	parallelism int,
	results []analyzeprofileschema.InvocationResult,
	outputOpts analyzeProfileResponseOptions,
) *analyzeprofileschema.Response {
	successCount := 0
	failedCount := 0
	summaries := make([]trace_events.Summary, 0, len(results))
	outputResults := make([]analyzeprofileschema.InvocationResult, len(results))

	sampleLimit := topN
	if sampleLimit <= 0 {
		sampleLimit = defaultTopN
	}
	if sampleLimit > 50 {
		sampleLimit = 50
	}
	incompleteProfileSamples := make([]string, 0, sampleLimit)
	mergedEventsSamples := make([]string, 0, sampleLimit)
	thresholds := insightThresholds()

	for i := range results {
		result := &results[i]
		outputResult := *result
		if result.Status != "ok" || result.Summary == nil {
			failedCount++
			outputResults[i] = outputResult
			continue
		}
		successCount++

		if result.Insights == nil {
			derived := trace_events.SummarizeTimingProfileInsights(*result.Summary, thresholds)
			result.Insights = &derived
		}
		if result.Diagnostics == nil {
			derived := trace_events.SummarizeTimingProfileDiagnostics(*result.Summary)
			result.Diagnostics = &derived
		}
		if result.Diagnostics.IncompleteProfile.IsIncomplete {
			incompleteProfileSamples = appendInvocationSample(incompleteProfileSamples, result.InvocationID, sampleLimit)
		}
		if result.Diagnostics.MergedEvents.HasMergedEvents {
			mergedEventsSamples = appendInvocationSample(mergedEventsSamples, result.InvocationID, sampleLimit)
		}

		outputResult.Metrics = invocationMetricsFromSummary(result.Summary)
		outputResult.Summary = projectedSummaryForOutput(result.Summary, outputOpts)
		outputResult.Insights = result.Insights
		outputResult.Diagnostics = result.Diagnostics
		outputResults[i] = outputResult

		summaries = append(summaries, *result.Summary)
	}

	aggregateSummary := trace_events.AggregateSummaryStats(summaries, topN)
	aggregateInsights := trace_events.AggregateTimingProfileInsightsFromSummaries(summaries, thresholds)
	aggregateDiagnostics := trace_events.AggregateTimingProfileDiagnosticsFromSummaries(summaries)

	response := &analyzeprofileschema.Response{
		RequestedInvocationCount: len(invocationIDs),
		AnalyzedInvocationCount:  successCount,
		FailedInvocationCount:    failedCount,
		TopN:                     topN,
		Parallelism:              parallelism,
		MaxOutputBytes:           outputOpts.MaxOutputBytes,
		Invocations:              outputResults,
		Insights:                 aggregateInsights,
		Diagnostics: analyzeprofileschema.AggregateTimingProfileDiagnostics{
			IncompleteProfile: analyzeprofileschema.AggregateTimingProfileIncompleteProfileDiagnostic{
				IncompleteInvocationCount: aggregateDiagnostics.IncompleteProfileCount,
				SampleInvocationIDs:       incompleteProfileSamples,
			},
			MergedEvents: analyzeprofileschema.AggregateTimingProfileMergedEventsDiagnostic{
				MergedEventsInvocationCount: aggregateDiagnostics.MergedEventsCount,
				SampleInvocationIDs:         mergedEventsSamples,
			},
		},
		Aggregate: aggregateSummary,
	}
	return applyAnalyzeProfileOutputLimit(response, outputOpts.MaxOutputBytes)
}

func invocationMetricsFromSummary(summary *trace_events.Summary) *analyzeprofileschema.InvocationMetrics {
	if summary == nil {
		return nil
	}
	return &analyzeprofileschema.InvocationMetrics{
		EventCount:           summary.EventCount,
		CompleteEventCount:   summary.CompleteEventCount,
		TotalDurationUsec:    summary.TotalDurationUsec,
		ConfiguredJobs:       summary.ConfiguredJobs,
		ConfiguredJobsSource: summary.ConfiguredJobsSource,
		ActionCount:          summary.ActionCount,
		SignalDurationsUsec:  summary.SignalDurationsUsec,
		HasMergedEvents:      summary.HasMergedEvents,
	}
}

func projectedSummaryForOutput(summary *trace_events.Summary, opts analyzeProfileResponseOptions) *trace_events.Summary {
	if summary == nil || !opts.IncludeSummary {
		return nil
	}
	projected := *summary
	if !opts.IncludeSummaryTopSpans {
		projected.TopSpansByDurationUsec = nil
	}
	if !opts.IncludeSummaryTopCategories {
		projected.TopCategoriesByDurationUsec = nil
	}
	if !opts.IncludeSummaryTopThreads {
		projected.TopThreadsByDurationUsec = nil
	}
	if !opts.IncludeSummaryCriticalPathComponents {
		projected.CriticalPathComponents = nil
	}
	return &projected
}

func applyAnalyzeProfileOutputLimit(response *analyzeprofileschema.Response, maxOutputBytes int) *analyzeprofileschema.Response {
	outputBytesBefore := setResponseOutputBytes(response)
	if maxOutputBytes <= 0 || outputBytesBefore <= maxOutputBytes {
		return response
	}

	droppedFields := make([]string, 0, 8)
	currentBytes := outputBytesBefore
	dropIfPresent := func(path string, fn func() bool) bool {
		if !fn() {
			return false
		}
		droppedFields = append(droppedFields, path)
		currentBytes = setResponseOutputBytes(response)
		return true
	}

	if currentBytes > maxOutputBytes {
		dropIfPresent("invocations[].summary.critical_path_components", func() bool {
			changed := false
			for i := range response.Invocations {
				summary := response.Invocations[i].Summary
				if summary == nil || len(summary.CriticalPathComponents) == 0 {
					continue
				}
				summary.CriticalPathComponents = nil
				changed = true
			}
			return changed
		})
	}
	if currentBytes > maxOutputBytes {
		dropIfPresent("invocations[].summary.top_threads_by_duration_usec", func() bool {
			changed := false
			for i := range response.Invocations {
				summary := response.Invocations[i].Summary
				if summary == nil || len(summary.TopThreadsByDurationUsec) == 0 {
					continue
				}
				summary.TopThreadsByDurationUsec = nil
				changed = true
			}
			return changed
		})
	}
	if currentBytes > maxOutputBytes {
		dropIfPresent("invocations[].summary.top_spans_by_duration_usec", func() bool {
			changed := false
			for i := range response.Invocations {
				summary := response.Invocations[i].Summary
				if summary == nil || len(summary.TopSpansByDurationUsec) == 0 {
					continue
				}
				summary.TopSpansByDurationUsec = nil
				changed = true
			}
			return changed
		})
	}
	if currentBytes > maxOutputBytes {
		dropIfPresent("invocations[].summary.top_categories_by_duration_usec", func() bool {
			changed := false
			for i := range response.Invocations {
				summary := response.Invocations[i].Summary
				if summary == nil || len(summary.TopCategoriesByDurationUsec) == 0 {
					continue
				}
				summary.TopCategoriesByDurationUsec = nil
				changed = true
			}
			return changed
		})
	}
	if currentBytes > maxOutputBytes {
		dropIfPresent("invocations[].summary", func() bool {
			changed := false
			for i := range response.Invocations {
				if response.Invocations[i].Summary == nil {
					continue
				}
				response.Invocations[i].Summary = nil
				changed = true
			}
			return changed
		})
	}
	if currentBytes > maxOutputBytes {
		dropIfPresent("invocations[].hints", func() bool {
			changed := false
			for i := range response.Invocations {
				if len(response.Invocations[i].Hints) == 0 {
					continue
				}
				response.Invocations[i].Hints = nil
				changed = true
			}
			return changed
		})
	}
	if currentBytes > maxOutputBytes {
		dropIfPresent("invocations[].profile_file.digest", func() bool {
			changed := false
			for i := range response.Invocations {
				profileFile := response.Invocations[i].ProfileFile
				if profileFile == nil || profileFile.Digest == nil {
					continue
				}
				profileFile.Digest = nil
				changed = true
			}
			return changed
		})
	}
	if currentBytes > maxOutputBytes {
		dropIfPresent("aggregate.top_spans_by_duration_usec", func() bool {
			if len(response.Aggregate.TopSpansByDurationUsec) == 0 {
				return false
			}
			response.Aggregate.TopSpansByDurationUsec = nil
			return true
		})
	}
	if currentBytes > maxOutputBytes {
		dropIfPresent("aggregate.top_categories_by_duration_usec", func() bool {
			if len(response.Aggregate.TopCategoriesByDurationUsec) == 0 {
				return false
			}
			response.Aggregate.TopCategoriesByDurationUsec = nil
			return true
		})
	}

	outputBytesAfter := setResponseOutputBytes(response)
	if outputBytesAfter < outputBytesBefore {
		response.Truncation = &analyzeprofileschema.OutputTruncation{
			OutputBytesBefore: outputBytesBefore,
			OutputBytesAfter:  outputBytesAfter,
			DroppedFields:     droppedFields,
		}
	}
	return response
}

func setResponseOutputBytes(response *analyzeprofileschema.Response) int {
	if response == nil {
		return 0
	}
	response.OutputBytes = 0
	size := responseJSONSize(response)
	response.OutputBytes = size
	finalSize := responseJSONSize(response)
	if finalSize != size {
		response.OutputBytes = finalSize
		return finalSize
	}
	return size
}

func responseJSONSize(response *analyzeprofileschema.Response) int {
	if response == nil {
		return 0
	}
	payload, err := json.Marshal(response)
	if err != nil {
		return 0
	}
	return len(payload)
}

func insightThresholds() trace_events.InsightThresholds {
	return trace_events.InsightThresholds{
		HighGCDurationPercentThreshold:            highGCDurationPercentThreshold,
		JobsCriticalPathDominancePercentThreshold: jobsCriticalPathDominancePercentThreshold,
		JobsUtilizationPercentThreshold:           jobsUtilizationPercentThreshold,
		QueueingBottleneckPercentThreshold:        queueingBottleneckPercentThreshold,
	}
}

func appendInvocationSample(samples []string, invocationID string, limit int) []string {
	if limit <= 0 || len(samples) >= limit {
		return samples
	}
	return append(samples, invocationID)
}

func invocationRefsFromArgs(args map[string]any) ([]string, error) {
	refs := make([]string, 0, 4)
	if invocationID, ok := args["invocation_id"]; ok && invocationID != nil {
		s, ok := invocationID.(string)
		if !ok {
			return nil, fmt.Errorf("\"invocation_id\" must be a string")
		}
		s = strings.TrimSpace(s)
		if s != "" {
			refs = append(refs, s)
		}
	}
	if invocationIDsValue, ok := args["invocation_ids"]; ok && invocationIDsValue != nil {
		parsed, err := stringSlice(invocationIDsValue, "invocation_ids")
		if err != nil {
			return nil, err
		}
		refs = append(refs, parsed...)
	}
	if len(refs) == 0 {
		return nil, fmt.Errorf("one of \"invocation_id\" or \"invocation_ids\" is required")
	}
	return refs, nil
}

func stringSlice(value any, field string) ([]string, error) {
	switch typed := value.(type) {
	case []any:
		out := make([]string, 0, len(typed))
		for i, item := range typed {
			s, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("\"%s[%d]\" must be a string", field, i)
			}
			s = strings.TrimSpace(s)
			if s == "" {
				return nil, fmt.Errorf("\"%s[%d]\" must be non-empty", field, i)
			}
			out = append(out, s)
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("\"%s\" must have at least one element", field)
		}
		return out, nil
	case []string:
		out := make([]string, 0, len(typed))
		for i, item := range typed {
			item = strings.TrimSpace(item)
			if item == "" {
				return nil, fmt.Errorf("\"%s[%d]\" must be non-empty", field, i)
			}
			out = append(out, item)
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("\"%s\" must have at least one element", field)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("\"%s\" must be an array of strings", field)
	}
}

func optionalStringSlice(args map[string]any, field string) ([]string, error) {
	value, ok := args[field]
	if !ok || value == nil {
		return nil, nil
	}
	return stringSlice(value, field)
}

func timingProfileFileSummary(file *bespb.File, rn *digest.CASResourceName) *analyzeprofileschema.ProfileFile {
	if file == nil {
		return nil
	}
	profileFile := &analyzeprofileschema.ProfileFile{
		Path:        filePath(file),
		Name:        file.GetName(),
		URI:         file.GetUri(),
		Digest:      file.GetDigest(),
		LengthBytes: file.GetLength(),
	}
	if rn != nil {
		profileFile.ResourceName = rn.DownloadString()
	}
	if symlinkTarget := file.GetSymlinkTargetPath(); symlinkTarget != "" {
		profileFile.SymlinkTargetPath = symlinkTarget
	}
	if contentsLen := len(file.GetContents()); contentsLen > 0 {
		profileFile.InlinedContentsBytes = contentsLen
	}
	return profileFile
}

func timingProfileErrorResult(invocationID string, err error) analyzeprofileschema.InvocationResult {
	return analyzeprofileschema.InvocationResult{
		InvocationID: invocationID,
		Status:       "error",
		Error:        err.Error(),
	}
}

func isContextDoneErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func trimOutputTail(raw []byte) string {
	const maxBytes = 4096
	raw = bytes.TrimSpace(raw)
	if len(raw) <= maxBytes {
		return string(raw)
	}
	return string(raw[len(raw)-maxBytes:])
}
