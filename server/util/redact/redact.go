package redact

import (
	"context"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/golang/protobuf/proto"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
)

const (
	RedactionFlagStandardRedactions = 1
)

var (
	urlSecretRegex = regexp.MustCompile(`[a-zA-Z-0-9-_=]+\@`)

	knownGitRepoURLKeys = []string{
		"REPO_URL", "GIT_URL", "TRAVIS_REPO_SLUG", "BUILDKITE_REPO",
		"CIRCLE_REPOSITORY_URL", "GITHUB_REPOSITORY", "CI_REPOSITORY_URL",
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

// StreamingRedactor processes a stream of build events and redacts them as they are
// received by the event handler.
type StreamingRedactor struct {
	env environment.Env
}

func NewStreamingRedactor(env environment.Env) *StreamingRedactor {
	return &StreamingRedactor{env}
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
			p.Started.OptionsDescription = stripURLSecrets(p.Started.OptionsDescription)
		}
	case *bespb.BuildEvent_UnstructuredCommandLine:
		{
			// Clear the unstructured command line so we don't have to redact it.
			p.UnstructuredCommandLine.Args = []string{}
		}
	case *bespb.BuildEvent_StructuredCommandLine:
		{
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
