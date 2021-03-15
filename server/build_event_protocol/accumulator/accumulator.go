package accumulator

import (
	"fmt"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/proto/command_line"
)

const (
	repoURLFieldName             = "repoURL"
	commitSHAFieldName           = "commitSHA"
	roleFieldName                = "role"
	commandFieldName             = "command"
	patternFieldName             = "pattern"
	workflowIDFieldName          = "workflowID"
	actionNameFieldName          = "actionName"
	actionTriggerEventFieldName  = "actionTriggerEvent"
	actionTriggerBranchFieldName = "actionTriggerBranch"
)

var (
	buildMetadataFieldMapping = map[string]string{
		"REPO_URL":                         repoURLFieldName,
		"COMMIT_SHA":                       commitSHAFieldName,
		"ROLE":                             roleFieldName,
		"BUILDBUDDY_WORKFLOW_ID":           workflowIDFieldName,
		"BUILDBUDDY_ACTION_NAME":           actionNameFieldName,
		"BUILDBUDDY_ACTION_TRIGGER_EVENT":  actionTriggerEventFieldName,
		"BUILDBUDDY_ACTION_TRIGGER_BRANCH": actionTriggerBranchFieldName,
	}
)

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
	invocationID            string
	valuesMap               map[string]string
	sawWorkspaceStatusEvent bool
}

func NewBEValues(invocationID string) *BEValues {
	return &BEValues{
		invocationID:            invocationID,
		valuesMap:               make(map[string]string, 0),
		sawWorkspaceStatusEvent: false,
	}
}

func (v *BEValues) AddEvent(event *build_event_stream.BuildEvent) {
	switch p := event.Payload.(type) {
	case *build_event_stream.BuildEvent_Started:
		v.populateWorkspaceInfoFromStartedEvent(event)
	case *build_event_stream.BuildEvent_StructuredCommandLine:
		v.populateWorkspaceInfoFromStructuredCommandLine(p.StructuredCommandLine)
	case *build_event_stream.BuildEvent_BuildMetadata:
		v.populateWorkspaceInfoFromBuildMetadata(p.BuildMetadata)
	case *build_event_stream.BuildEvent_WorkspaceStatus:
		v.populateWorkspaceInfoFromWorkspaceStatus(p.WorkspaceStatus)
		v.sawWorkspaceStatusEvent = true
	}
}
func (v *BEValues) InvocationID() string {
	return v.invocationID
}

func (v *BEValues) RepoURL() string {
	return v.getStringValue(repoURLFieldName)
}

func (v *BEValues) CommitSHA() string {
	return v.getStringValue(commitSHAFieldName)
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

func (v *BEValues) ActionTriggerEvent() string {
	return v.getStringValue(actionTriggerEventFieldName)
}

func (v *BEValues) ActionTriggerBranch() string {
	return v.getStringValue(actionTriggerBranchFieldName)
}

func (v *BEValues) WorkspaceIsLoaded() bool {
	return v.sawWorkspaceStatusEvent
}

func (v *BEValues) getStringValue(fieldName string) string {
	if existing, ok := v.valuesMap[fieldName]; ok {
		return existing
	}
	return ""
}

func (v *BEValues) setStringValue(fieldName, proposedValue string) bool {
	existing, ok := v.valuesMap[fieldName]
	if ok && existing != "" {
		return false
	}
	v.valuesMap[fieldName] = proposedValue
	return true
}

func (v *BEValues) populateWorkspaceInfoFromStartedEvent(event *build_event_stream.BuildEvent) {
	v.setStringValue(commandFieldName, event.GetStarted().Command)
	v.setStringValue(patternFieldName, patternFromEvent(event))
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
			case "CIRCLE_SHA1", "GITHUB_SHA", "BUILDKITE_COMMIT", "TRAVIS_COMMIT", "GIT_COMMIT", "CI_COMMIT_SHA", "COMMIT_SHA":
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
		if item.Key == "COMMIT_SHA" {
			v.setStringValue(commitSHAFieldName, item.Value)
		}
	}
}

// TODO(siggisim): pull this out somewhere central
func truncatedJoin(list []string, maxItems int) string {
	length := len(list)
	if length > maxItems {
		return fmt.Sprintf("%s and %d more", strings.Join(list[0:maxItems], ", "), length-maxItems)
	}
	return strings.Join(list, ", ")
}

func patternFromEvent(event *build_event_stream.BuildEvent) string {
	for _, child := range event.Children {
		switch c := child.Id.(type) {
		case *build_event_stream.BuildEventId_Pattern:
			{
				return truncatedJoin(c.Pattern.Pattern, 3)
			}
		}
	}
	return ""
}
