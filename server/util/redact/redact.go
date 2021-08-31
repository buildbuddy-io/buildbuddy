package redact

import (
	"context"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/golang/protobuf/proto"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	clpb "github.com/buildbuddy-io/buildbuddy/proto/command_line"
	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
)

const (
	RedactionFlagStandardRedactions = 1

	envVarPrefix              = "--"
	envVarOptionName          = "client_env"
	envVarSeparator           = "="
	envVarRedactedPlaceholder = "<REDACTED>"

	buildMetadataOptionPrefix = "--build_metadata="
	allowEnvPrefix            = "ALLOW_ENV="
	allowEnvListSeparator     = ","
)

var (
	urlSecretRegex = regexp.MustCompile(`[a-zA-Z-0-9-_=]+\@`)

	knownGitRepoURLKeys = []string{
		"REPO_URL", "GIT_URL", "TRAVIS_REPO_SLUG", "BUILDKITE_REPO",
		"CIRCLE_REPOSITORY_URL", "GITHUB_REPOSITORY", "CI_REPOSITORY_URL",
	}

	defaultAllowedEnvVars = []string{
		"USER", "GITHUB_ACTOR", "GITHUB_SHA", "GITHUB_RUN_ID",
		"BUILDKITE_BUILD_URL", "BUILDKITE_JOB_ID", "CIRCLE_REPOSITORY_URL",
		"GITHUB_REPOSITORY", "BUILDKITE_REPO", "TRAVIS_REPO_SLUG", "GIT_URL",
		"CI_REPOSITORY_URL", "REPO_URL", "CIRCLE_SHA1", "BUILDKITE_COMMIT",
		"TRAVIS_COMMIT", "GIT_COMMIT", "CI_COMMIT_SHA", "COMMIT_SHA", "CI", "CI_RUNNER",
		"CIRCLE_BRANCH", "GITHUB_HEAD_REF", "BUILDKITE_BRANCH", "TRAVIS_BRANCH",
		"GIT_BRANCH", "CI_COMMIT_BRANCH", "GITHUB_REF",
	}

	redactedPlatformProps = []string{
		"container-registry-username", "container-registry-password",
	}
)

func stripURLSecrets(input string) string {
	return urlSecretRegex.ReplaceAllString(input, "")
}

func stripURLSecretsFromList(inputs []string) []string {
	for index, input := range inputs {
		inputs[index] = stripURLSecrets(input)
	}
	return inputs
}

func stripURLSecretsFromFile(file *bespb.File) *bespb.File {
	switch p := file.GetFile().(type) {
	case *bespb.File_Uri:
		p.Uri = stripURLSecrets(p.Uri)
	}
	return file
}

func stripURLSecretsFromFiles(files []*bespb.File) []*bespb.File {
	for index, file := range files {
		files[index] = stripURLSecretsFromFile(file)
	}
	return files
}

func stripRepoURLCredentialsFromBuildMetadata(metadata *bespb.BuildMetadata) {
	for _, repoURLKey := range knownGitRepoURLKeys {
		if val := metadata.Metadata[repoURLKey]; val != "" {
			metadata.Metadata[repoURLKey] = gitutil.StripRepoURLCredentials(val)
		}
	}
}

func stripRepoURLCredentialsFromWorkspaceStatus(status *bespb.WorkspaceStatus) {
	for _, item := range status.Item {
		if item.Value == "" {
			continue
		}
		for _, repoURLKey := range knownGitRepoURLKeys {
			if item.Key == repoURLKey {
				item.Value = gitutil.StripRepoURLCredentials(item.Value)
				break
			}
		}
	}
}

func stripRepoURLCredentialsFromCommandLineOption(option *clpb.Option) {
	// Only strip repo URLs from env var options that point to known Git env
	// vars; other options will be stripped using the regex-based method.
	if option.OptionName != envVarOptionName {
		return
	}
	for _, repoURLKey := range knownGitRepoURLKeys {
		// assignmentPrefix is a string like "REPO_URL=" or "GIT_URL="
		assignmentPrefix := repoURLKey + envVarSeparator
		if strings.HasPrefix(option.OptionValue, assignmentPrefix) {
			envVarValue := strings.TrimPrefix(option.OptionValue, assignmentPrefix)
			strippedValue := gitutil.StripRepoURLCredentials(envVarValue)
			option.OptionValue = assignmentPrefix + strippedValue
			option.CombinedForm = envVarPrefix + envVarOptionName + envVarSeparator + option.OptionValue
			return
		}
	}
}

func filterCommandLineOptions(options []*clpb.Option) []*clpb.Option {
	filtered := []*clpb.Option{}
	for _, option := range options {
		// Remove default_overrides for now since we don't have a use for them (yet)
		// and they may contain sensitive info.
		if option.OptionName == "default_override" {
			continue
		}
		filtered = append(filtered, option)
	}
	return filtered
}

func redactStructuredCommandLine(commandLine *clpb.CommandLine, allowedEnvVars []string) {
	for _, section := range commandLine.Sections {
		p, ok := section.SectionType.(*clpb.CommandLineSection_OptionList)
		if !ok {
			continue
		}
		p.OptionList.Option = filterCommandLineOptions(p.OptionList.Option)
		for _, option := range p.OptionList.Option {
			// Strip URL secrets. Strip git URLs explicitly first, since
			// gitutil.StripRepoURLCredentials is URL-aware. Then fall back to
			// regex-based stripping.
			stripRepoURLCredentialsFromCommandLineOption(option)
			option.OptionValue = stripURLSecrets(option.OptionValue)
			option.CombinedForm = stripURLSecrets(option.CombinedForm)

			// Redact remote header values
			if option.OptionName == "remote_header" || option.OptionName == "remote_cache_header" {
				option.OptionValue = envVarRedactedPlaceholder
				option.CombinedForm = envVarPrefix + option.OptionName + envVarSeparator + envVarRedactedPlaceholder
			}

			// Redact non-allowed env vars
			if option.OptionName == envVarOptionName {
				parts := strings.Split(option.OptionValue, envVarSeparator)
				if len(parts) == 0 || isAllowedEnvVar(parts[0], allowedEnvVars) {
					continue
				}
				option.OptionValue = parts[0] + envVarSeparator + envVarRedactedPlaceholder
				option.CombinedForm = envVarPrefix + envVarOptionName + envVarSeparator + parts[0] + envVarSeparator + envVarRedactedPlaceholder
			}

			// Redact sensitive platform props
			if option.OptionName == "remote_default_exec_properties" {
				for _, propName := range redactedPlatformProps {
					if strings.HasPrefix(option.OptionValue, propName+"=") {
						option.OptionValue = propName + "=<REDACTED>"
						option.CombinedForm = "--remote_default_exec_properties=" + propName + "=<REDACTED>"
					}
				}
			}
		}
	}
}

func isAllowedEnvVar(variableName string, allowedEnvVars []string) bool {
	lowercaseVariableName := strings.ToLower(variableName)
	for _, allowed := range allowedEnvVars {
		lowercaseAllowed := strings.ToLower(allowed)
		if allowed == "*" || lowercaseVariableName == lowercaseAllowed {
			return true
		}
		isWildCard := strings.HasSuffix(allowed, "*")
		allowedPrefix := strings.ReplaceAll(lowercaseAllowed, "*", "")
		if isWildCard && strings.HasPrefix(lowercaseVariableName, allowedPrefix) {
			return true
		}
	}
	return false
}

// parseAllowedEnv looks for an option like "--build_metadata='ALLOW_ENV=A,B,C'"
// and returns a slice of the comma-separated values specified in the value of
// ALLOW_ENV -- in this example, it would return {"A", "B", "C"}.
func parseAllowedEnv(optionsDescription string) []string {
	options := strings.Split(optionsDescription, " ")
	for _, option := range options {
		if !strings.HasPrefix(option, buildMetadataOptionPrefix) {
			continue
		}
		optionValue := strings.TrimPrefix(option, buildMetadataOptionPrefix)
		if strings.HasPrefix(optionValue, "'") && strings.HasSuffix(optionValue, "'") {
			optionValue = strings.TrimPrefix(optionValue, "'")
			optionValue = strings.TrimSuffix(optionValue, "'")
		}
		if !strings.HasPrefix(optionValue, allowEnvPrefix) {
			continue
		}
		envVarValue := strings.TrimPrefix(optionValue, allowEnvPrefix)
		return strings.Split(envVarValue, allowEnvListSeparator)
	}

	return []string{}
}

// StreamingRedactor processes a stream of build events and redacts them as they are
// received by the event handler.
type StreamingRedactor struct {
	env            environment.Env
	allowedEnvVars []string
}

func NewStreamingRedactor(env environment.Env) *StreamingRedactor {
	return &StreamingRedactor{
		env:            env,
		allowedEnvVars: defaultAllowedEnvVars,
	}
}

func (r *StreamingRedactor) RedactMetadata(event *bespb.BuildEvent) {
	switch p := event.Payload.(type) {
	case *bespb.BuildEvent_Progress:
		{
		}
	case *bespb.BuildEvent_Aborted:
		{
		}
	case *bespb.BuildEvent_Started:
		{
			r.allowedEnvVars = append(r.allowedEnvVars, parseAllowedEnv(p.Started.OptionsDescription)...)
			// Redact the whole options description to avoid having to parse individual
			// options. The StructuredCommandLine should contain all of these options
			// anyway.
			p.Started.OptionsDescription = "<REDACTED>"
		}
	case *bespb.BuildEvent_UnstructuredCommandLine:
		{
			// Clear the unstructured command line so we don't have to redact it.
			p.UnstructuredCommandLine.Args = []string{}
		}
	case *bespb.BuildEvent_StructuredCommandLine:
		{
			redactStructuredCommandLine(p.StructuredCommandLine, r.allowedEnvVars)
		}
	case *bespb.BuildEvent_OptionsParsed:
		{
			p.OptionsParsed.CmdLine = stripURLSecretsFromList(p.OptionsParsed.CmdLine)
			p.OptionsParsed.ExplicitCmdLine = stripURLSecretsFromList(p.OptionsParsed.ExplicitCmdLine)
		}
	case *bespb.BuildEvent_WorkspaceStatus:
		{
			stripRepoURLCredentialsFromWorkspaceStatus(p.WorkspaceStatus)
		}
	case *bespb.BuildEvent_Fetch:
		{
		}
	case *bespb.BuildEvent_Configuration:
		{
		}
	case *bespb.BuildEvent_Expanded:
		{
		}
	case *bespb.BuildEvent_Configured:
		{
		}
	case *bespb.BuildEvent_Action:
		{
			p.Action.Stdout = stripURLSecretsFromFile(p.Action.Stdout)
			p.Action.Stderr = stripURLSecretsFromFile(p.Action.Stderr)
			p.Action.PrimaryOutput = stripURLSecretsFromFile(p.Action.PrimaryOutput)
			p.Action.ActionMetadataLogs = stripURLSecretsFromFiles(p.Action.ActionMetadataLogs)
		}
	case *bespb.BuildEvent_NamedSetOfFiles:
		{
			p.NamedSetOfFiles.Files = stripURLSecretsFromFiles(p.NamedSetOfFiles.Files)
		}
	case *bespb.BuildEvent_Completed:
		{
			p.Completed.ImportantOutput = stripURLSecretsFromFiles(p.Completed.ImportantOutput)
		}
	case *bespb.BuildEvent_TestResult:
		{
			p.TestResult.TestActionOutput = stripURLSecretsFromFiles(p.TestResult.TestActionOutput)
		}
	case *bespb.BuildEvent_TestSummary:
		{
			p.TestSummary.Passed = stripURLSecretsFromFiles(p.TestSummary.Passed)
			p.TestSummary.Failed = stripURLSecretsFromFiles(p.TestSummary.Failed)
		}
	case *bespb.BuildEvent_Finished:
		{
		}
	case *bespb.BuildEvent_BuildToolLogs:
		{
			p.BuildToolLogs.Log = stripURLSecretsFromFiles(p.BuildToolLogs.Log)
		}
	case *bespb.BuildEvent_BuildMetrics:
		{
		}
	case *bespb.BuildEvent_WorkspaceInfo:
		{
		}
	case *bespb.BuildEvent_BuildMetadata:
		{
			stripRepoURLCredentialsFromBuildMetadata(p.BuildMetadata)
		}
	case *bespb.BuildEvent_ConvenienceSymlinksIdentified:
		{
		}
	case *bespb.BuildEvent_WorkflowConfigured:
		{
		}
	}
	return
}

func (r *StreamingRedactor) RedactAPIKey(ctx context.Context, event *bespb.BuildEvent) error {
	proto.DiscardUnknown(event)

	apiKey, ok := ctx.Value("x-buildbuddy-api-key").(string)
	if !ok {
		return nil
	}

	txt := proto.MarshalTextString(event)
	// TODO: Show the display label of the API key that was redacted, if we can
	// get that info efficiently.
	txt = strings.ReplaceAll(txt, apiKey, "<REDACTED>")

	return proto.UnmarshalText(txt, event)
}

func (r *StreamingRedactor) RedactAPIKeysWithSlowRegexp(ctx context.Context, event *bespb.BuildEvent) error {
	proto.DiscardUnknown(event)
	txt := proto.MarshalTextString(event)

	// NB: this implementation depends on the way we generate API keys
	// (20 alphanumeric characters).

	// Replace x-buildbuddy-api-key header.
	pat := regexp.MustCompile("x-buildbuddy-api-key=[[:alnum:]]{20}")
	txt = pat.ReplaceAllLiteralString(txt, "x-buildbuddy-api-key=<REDACTED>")

	// Replace sequences that look like API keys immediately followed by '@',
	// to account for patterns like "grpc://$API_KEY@app.buildbuddy.io"
	// or "bes_backend=$API_KEY@domain.com".

	// Here we match 20 alphanum chars occurring at the start of a line.
	pat = regexp.MustCompile("^[[:alnum:]]{20}@")
	txt = pat.ReplaceAllLiteralString(txt, "<REDACTED>@")
	// Here we match 20 alphanum chars anywhere in the line, preceded by a non-
	// alphanum char (to ensure the match is exactly 20 alphanum chars long).
	pat = regexp.MustCompile("([^[:alnum:]])[[:alnum:]]{20}@")
	txt = pat.ReplaceAllString(txt, "$1<REDACTED>@")

	// Replace the literal API key set up via the BuildBuddy config, which does not
	// need to conform to the way we generate API keys.
	configuredKey := getAPIKeyFromBuildBuddyConfig(r.env)
	if configuredKey != "" {
		txt = strings.ReplaceAll(txt, configuredKey, "<REDACTED>")
	}

	contextKey, ok := ctx.Value("x-buildbuddy-api-key").(string)
	if ok {
		txt = strings.ReplaceAll(txt, contextKey, "<REDACTED>")
	}

	return proto.UnmarshalText(txt, event)
}

// Returns the API key hard-coded in the BuildBuddy config file / config flags,
// or "" if there is no key configured.
func getAPIKeyFromBuildBuddyConfig(env environment.Env) string {
	if apiConfig := env.GetConfigurator().GetAPIConfig(); apiConfig != nil {
		return apiConfig.APIKey
	}
	return ""
}
