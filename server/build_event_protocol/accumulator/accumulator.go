package accumulator

import (
	"context"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/proto/command_line"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/event_parser"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/invocation_format"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
)

const (
	repoURLFieldName                      = "repoURL"
	branchNameFieldName                   = "branchName"
	commitSHAFieldName                    = "commitSHA"
	roleFieldName                         = "role"
	commandFieldName                      = "command"
	patternFieldName                      = "pattern"
	workflowIDFieldName                   = "workflowID"
	actionNameFieldName                   = "actionName"
	disableCommitStatusReportingFieldName = "disableCommitStatusReporting"
)

var (
	buildMetadataFieldMapping = map[string]string{
		"REPO_URL":                        repoURLFieldName,
		"BRANCH_NAME":                     branchNameFieldName,
		"COMMIT_SHA":                      commitSHAFieldName,
		"ROLE":                            roleFieldName,
		"DISABLE_COMMIT_STATUS_REPORTING": disableCommitStatusReportingFieldName,
	}
)

type Accumulator interface {
	// Invocation returns the accumulated invocation proto. Not all fields may
	// be populated yet if the build is in progress. Only the "summary" fields
	// such as command, pattern, etc. will be set. Fields that scale with the
	// number of events received (such as the Events list or console buffer) are
	// not present.
	Invocation() *inpb.Invocation

	// TODO(bduffany): Remove these methods in favor of reading them directly
	// from the Invocation() proto fields.
	InvocationID() string
	StartTime() time.Time
	RepoURL() string
	BranchName() string
	CommitSHA() string
	DisableCommitStatusReporting() bool
	Role() string
	Command() string
	Pattern() string
	WorkflowID() string
	ActionName() string
	WorkspaceIsLoaded() bool
	BuildMetadataIsLoaded() bool
	BuildFinished() bool
}

// BEValues is an in-memory data structure created for each new stream of build
// events. Every event in the stream is passed through BEValues, which extracts
// common values that many functions are interested in. By holding a reference
// to BEValues, those functions can obtain those common values without
// duplicating the BES parsing logic.
//
// N.B. Commonly extracted values should be added here. BEValues is held in
// memory for the life of the stream, so it should not save every single event
// in full (that data lives in blobstore).
type BEValues struct {
	valuesMap               map[string]string
	sawWorkspaceStatusEvent bool
	sawBuildMetadataEvent   bool
	buildStartTime          time.Time
	exitCode                *string

	// TODO(bduffany): Migrate all parser functionality directly into the
	// accumulator. The parser is a separate entity only for historical reasons.
	parser *event_parser.StreamingEventParser
}

func NewBEValues(invocation *inpb.Invocation) *BEValues {
	return &BEValues{
		valuesMap: make(map[string]string, 0),
		parser:    event_parser.NewStreamingEventParser(invocation),
	}
}

func (v *BEValues) Invocation() *inpb.Invocation {
	return v.parser.GetInvocation()
}

func (v *BEValues) AddEvent(event *build_event_stream.BuildEvent) {
	v.parser.ParseEvent(event)

	switch p := event.Payload.(type) {
	case *build_event_stream.BuildEvent_Started:
		v.handleStartedEvent(event)
	case *build_event_stream.BuildEvent_StructuredCommandLine:
		v.populateWorkspaceInfoFromStructuredCommandLine(p.StructuredCommandLine)
	case *build_event_stream.BuildEvent_BuildMetadata:
		v.populateWorkspaceInfoFromBuildMetadata(p.BuildMetadata)
		v.sawBuildMetadataEvent = true
	case *build_event_stream.BuildEvent_WorkspaceStatus:
		v.populateWorkspaceInfoFromWorkspaceStatus(p.WorkspaceStatus)
		v.sawWorkspaceStatusEvent = true
	case *build_event_stream.BuildEvent_WorkflowConfigured:
		v.handleWorkflowConfigured(p.WorkflowConfigured)
	case *build_event_stream.BuildEvent_Finished:
		v.handleFinishedEvent(p.Finished)
	}
}
func (v *BEValues) Finalize(ctx context.Context) {
	invocation := v.Invocation()
	invocation.InvocationStatus = inpb.Invocation_DISCONNECTED_INVOCATION_STATUS
	if v.BuildFinished() {
		invocation.InvocationStatus = inpb.Invocation_COMPLETE_INVOCATION_STATUS
	}

	// Do some logging so that we can understand whether the invocation fields
	// observed by BuildStatusReporter / TargetTracker differ from the fields
	// that we store in the DB. (This code is part of a refactor that aims
	// to consolidate EventParser and Accumulator).
	// TODO(bduffany): Decide how to reconcile any differences logged here, and
	// remove this logging.
	if v.RepoURL() != invocation.GetRepoUrl() {
		log.CtxInfof(ctx, "Accumulator: repo_url mismatch: %q != %q", v.RepoURL(), invocation.GetRepoUrl())
	}
	if v.CommitSHA() != invocation.GetCommitSha() {
		log.CtxInfof(ctx, "Accumulator: commit_sha mismatch: %q != %q", v.CommitSHA(), invocation.GetCommitSha())
	}
	if v.Role() != invocation.GetRole() {
		log.CtxInfof(ctx, "Accumulator: role mismatch: %q != %q", v.Role(), invocation.GetRole())
	}
}
func (v *BEValues) InvocationID() string {
	return v.Invocation().GetInvocationId()
}
func (v *BEValues) StartTime() time.Time {
	return v.buildStartTime
}

// RepoURL returns the normalized repo URL.
func (v *BEValues) RepoURL() string {
	rawURL := v.getStringValue(repoURLFieldName)
	normalizedURL, err := gitutil.NormalizeRepoURL(rawURL)
	if err != nil {
		return rawURL
	}
	return normalizedURL.String()
}

func (v *BEValues) BranchName() string {
	return v.getStringValue(branchNameFieldName)
}

func (v *BEValues) CommitSHA() string {
	return v.getStringValue(commitSHAFieldName)
}

func (v *BEValues) DisableCommitStatusReporting() bool {
	return v.getBoolValue(disableCommitStatusReportingFieldName)
}

func (v *BEValues) Role() string {
	return v.getStringValue(roleFieldName)
}

func (v *BEValues) Command() string {
	return v.getStringValue(commandFieldName)
}

func (v *BEValues) Pattern() string {
	return v.getStringValue(patternFieldName)
}

func (v *BEValues) WorkflowID() string {
	return v.getStringValue(workflowIDFieldName)
}

func (v *BEValues) ActionName() string {
	return v.getStringValue(actionNameFieldName)
}

func (v *BEValues) WorkspaceIsLoaded() bool {
	return v.sawWorkspaceStatusEvent
}

func (v *BEValues) BuildMetadataIsLoaded() bool {
	return v.sawBuildMetadataEvent
}

func (v *BEValues) BuildFinished() bool {
	return v.exitCode != nil
}

func (v *BEValues) BuildExitCode() string {
	if v.exitCode == nil {
		return ""
	}
	return *v.exitCode
}

func (v *BEValues) getStringValue(fieldName string) string {
	if existing, ok := v.valuesMap[fieldName]; ok {
		return existing
	}
	return ""
}

func (v *BEValues) setStringValue(fieldName, proposedValue string) bool {
	if fieldName == repoURLFieldName {
		proposedValue = gitutil.StripRepoURLCredentials(proposedValue)
	}

	existing, ok := v.valuesMap[fieldName]
	if ok && existing != "" {
		return false
	}
	v.valuesMap[fieldName] = proposedValue
	return true
}

func (v *BEValues) getBoolValue(fieldName string) bool {
	val := v.getStringValue(fieldName)
	return val == "true" || val == "True" || val == "TRUE" || val == "yes" || val == "1"
}

func (v *BEValues) handleStartedEvent(event *build_event_stream.BuildEvent) {
	v.setStringValue(commandFieldName, event.GetStarted().Command)
	v.setStringValue(patternFieldName, patternFromEvent(event))
	v.buildStartTime = timeutil.GetTimeWithFallback(event.GetStarted().GetStartTime(), event.GetStarted().GetStartTimeMillis())
}

func (v *BEValues) populateWorkspaceInfoFromStructuredCommandLine(commandLine *command_line.CommandLine) {
	for _, section := range commandLine.Sections {
		if list := section.GetOptionList(); list == nil {
			continue
		}
		for _, option := range section.GetOptionList().Option {
			if option.OptionName != "ENV" && option.OptionName != "client_env" {
				continue
			}
			parts := strings.Split(option.OptionValue, "=")
			if len(parts) != 2 {
				continue
			}
			environmentVariable := parts[0]
			value := parts[1]
			switch environmentVariable {
			case "CIRCLE_REPOSITORY_URL", "GITHUB_REPOSITORY", "BUILDKITE_REPO", "TRAVIS_REPO_SLUG", "GIT_URL", "CI_REPOSITORY_URL", "REPO_URL":
				v.setStringValue(repoURLFieldName, value)
			case "CIRCLE_BRANCH", "GITHUB_HEAD_REF", "BUILDKITE_BRANCH", "TRAVIS_BRANCH", "GIT_BRANCH", "CI_COMMIT_BRANCH":
				v.setStringValue(branchNameFieldName, value)
			case "GITHUB_REF":
				// GITHUB_REF can contain tag information instead of branch information,
				// so we have to check that this is a branch before attempting to strip
				// the prefix. Additionally, if this is a pull request, we want to use
				// the value from GITHUB_HEAD_REF instead, so if the branch name is
				// already populated, we don't overwrite it.
				if strings.HasPrefix(value, "refs/heads/") && v.getStringValue(branchNameFieldName) == "" {
					v.setStringValue(branchNameFieldName, strings.TrimPrefix(value, "refs/heads/"))
				}
			case "CIRCLE_SHA1", "GITHUB_SHA", "BUILDKITE_COMMIT", "TRAVIS_COMMIT", "GIT_COMMIT", "CI_COMMIT_SHA", "COMMIT_SHA", "VOLATILE_GIT_COMMIT":
				v.setStringValue(commitSHAFieldName, value)
			case "CI":
				v.setStringValue(roleFieldName, "CI")
			case "CI_RUNNER":
				v.setStringValue(roleFieldName, "CI_RUNNER")
			}
		}
	}
}

func (v *BEValues) populateWorkspaceInfoFromBuildMetadata(metadata *build_event_stream.BuildMetadata) {
	for mdKey, mdVal := range metadata.Metadata {
		if fieldName := buildMetadataFieldMapping[mdKey]; fieldName != "" {
			v.setStringValue(fieldName, mdVal)
		}
	}
}

func (v *BEValues) populateWorkspaceInfoFromWorkspaceStatus(workspace *build_event_stream.WorkspaceStatus) {
	for _, item := range workspace.Item {
		if item.Key == "REPO_URL" {
			v.setStringValue(repoURLFieldName, item.Value)
		}
		if item.Key == "BRANCH_NAME" {
			v.setStringValue(branchNameFieldName, item.Value)
		}
		if item.Key == "COMMIT_SHA" {
			v.setStringValue(commitSHAFieldName, item.Value)
		}
	}
}

func (v *BEValues) handleWorkflowConfigured(wfc *build_event_stream.WorkflowConfigured) {
	v.setStringValue(workflowIDFieldName, wfc.GetWorkflowId())
	v.setStringValue(actionNameFieldName, wfc.GetActionName())
}

func (v *BEValues) handleFinishedEvent(finished *build_event_stream.BuildFinished) {
	v.exitCode = new(string)
	*v.exitCode = finished.ExitCode.GetName()
}

func patternFromEvent(event *build_event_stream.BuildEvent) string {
	for _, child := range event.Children {
		switch c := child.Id.(type) {
		case *build_event_stream.BuildEventId_Pattern:
			{
				return invocation_format.ShortFormatPatterns(c.Pattern.Pattern)
			}
		}
	}
	return ""
}
