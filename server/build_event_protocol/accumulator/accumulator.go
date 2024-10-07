package accumulator

import (
	"context"
	"net/url"
	"regexp"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/event_parser"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/invocation_format"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

const (
	workflowIDFieldName                   = "workflowID"
	actionNameFieldName                   = "actionName"
	disableCommitStatusReportingFieldName = "disableCommitStatusReporting"
	disableTargetTrackingFieldName        = "disableTargetTracking"

	// The maximum number of important files and artifacts to possibly copy
	// from cache -> blobstore. If more than this number are present, they
	// will be dropped.
	maxPersistableArtifacts = 1000

	// If codesearch is enabled, and an invocation contains a single file with the
	// following name, attempt to ingest this kythe sstable file in codesearch.
	KytheOutputName = "kythe_serving.sst"
)

var (
	buildMetadataFieldMapping = map[string]string{
		"DISABLE_COMMIT_STATUS_REPORTING": disableCommitStatusReportingFieldName,
		"DISABLE_TARGET_TRACKING":         disableTargetTrackingFieldName,
	}
	bytestreamURIPattern = regexp.MustCompile(`^bytestream://.*/blobs/([a-z0-9]{64})/\d+$`)
)

type Accumulator interface {
	// Invocation returns the accumulated invocation proto. Not all fields may
	// be populated yet if the build is in progress. Only the "summary" fields
	// such as command, pattern, etc. will be set. Fields that scale with the
	// number of events received (such as the Events list or console buffer) are
	// not present.
	Invocation() *inpb.Invocation

	// MetadataIsLoaded returns whether all expected invocation-level metadata
	// events have been received. It's generally not safe to call the other
	// methods such as Pattern() or WorkflowID() without first checking whether
	// metadata is loaded.
	MetadataIsLoaded() bool

	StartTime() time.Time
	DisableCommitStatusReporting() bool
	DisableTargetTracking() bool
	WorkflowID() string
	ActionName() string
	Pattern() string

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
	valuesMap                      map[string]string
	unprocessedMetadataEvents      map[string]struct{}
	sawStartedEvent                bool
	sawFinishedEvent               bool
	buildStartTime                 time.Time
	buildToolLogURIs               []*url.URL
	outputFilesMap                 map[string]*build_event_stream.File
	kytheSSTableResourceName       *rspb.ResourceName
	profileName                    string
	hasBytestreamTestActionOutputs bool

	testOutputURIs []*url.URL
	// TODO(bduffany): Migrate all parser functionality directly into the
	// accumulator. The parser is a separate entity only for historical reasons.
	parser *event_parser.StreamingEventParser
}

func NewBEValues(invocation *inpb.Invocation) *BEValues {
	return &BEValues{
		valuesMap:                 make(map[string]string, 0),
		unprocessedMetadataEvents: make(map[string]struct{}, 0),
		outputFilesMap:            make(map[string]*build_event_stream.File),
		parser:                    event_parser.NewStreamingEventParser(invocation),
	}
}

func (v *BEValues) Invocation() *inpb.Invocation {
	return v.parser.GetInvocation()
}

func (v *BEValues) maybeExtractOutputFile(files ...*build_event_stream.File) {
	for _, file := range files {
		if file.GetName() == "" {
			continue
		}
		if m := bytestreamURIPattern.FindStringSubmatch(file.GetUri()); len(m) >= 1 {
			digestHash := m[1]
			v.outputFilesMap[digestHash] = file
		}
		// Special case: check for kythe output files.
		if file.GetName() == KytheOutputName {
			uri, err := url.Parse(file.GetUri())
			if err != nil {
				continue
			}
			rn, err := digest.ParseDownloadResourceName(uri.Path)
			if err != nil {
				continue
			}
			v.kytheSSTableResourceName = rn.ToProto()
		}
	}
}

func (v *BEValues) AddEvent(event *build_event_stream.BuildEvent) error {
	if err := v.parser.ParseEvent(event); err != nil {
		return err
	}

	// Update the set of metadata events that we're still expecting.
	if IsMetadataEvent(event.GetId()) {
		delete(v.unprocessedMetadataEvents, event.GetId().String())
	}

	switch p := event.Payload.(type) {
	case *build_event_stream.BuildEvent_NamedSetOfFiles:
		v.maybeExtractOutputFile(p.NamedSetOfFiles.GetFiles()...)
	case *build_event_stream.BuildEvent_TestSummary:
		v.maybeExtractOutputFile(p.TestSummary.GetPassed()...)
		v.maybeExtractOutputFile(p.TestSummary.GetFailed()...)
	case *build_event_stream.BuildEvent_RunTargetAnalyzed:
		v.maybeExtractOutputFile(p.RunTargetAnalyzed.GetRunfiles()...)
	case *build_event_stream.BuildEvent_Action:
		v.maybeExtractOutputFile(p.Action.GetStdout())
		v.maybeExtractOutputFile(p.Action.GetStderr())
		v.maybeExtractOutputFile(p.Action.GetPrimaryOutput())
		v.maybeExtractOutputFile(p.Action.GetActionMetadataLogs()...)
	case *build_event_stream.BuildEvent_Completed:
		v.maybeExtractOutputFile(p.Completed.GetImportantOutput()...)
		v.maybeExtractOutputFile(p.Completed.GetDirectoryOutput()...)
	case *build_event_stream.BuildEvent_Started:
		v.handleStartedEvent(event)
	case *build_event_stream.BuildEvent_BuildMetadata:
		v.populateWorkspaceInfoFromBuildMetadata(p.BuildMetadata)
	case *build_event_stream.BuildEvent_WorkflowConfigured:
		v.handleWorkflowConfigured(p.WorkflowConfigured)
	case *build_event_stream.BuildEvent_Finished:
		v.sawFinishedEvent = true
	case *build_event_stream.BuildEvent_BuildToolLogs:
		v.maybeExtractOutputFile(p.BuildToolLogs.GetLog()...)
		for _, toolLog := range p.BuildToolLogs.Log {
			if uri := toolLog.GetUri(); uri != "" {
				if url, err := url.Parse(uri); err != nil {
					log.Warningf("Error parsing uri from BuildToolLogs: %s", uri)
				} else if url.Scheme == "bytestream" {
					v.buildToolLogURIs = append(v.buildToolLogURIs, url)
				}
			}
		}
	case *build_event_stream.BuildEvent_TestResult:
		v.maybeExtractOutputFile(p.TestResult.GetTestActionOutput()...)
		for _, f := range p.TestResult.TestActionOutput {
			u, err := url.Parse(f.GetUri())
			if err != nil {
				log.Warningf("Error parsing uri from TestResult: %s", f.GetUri())
				continue
			}
			if u.Scheme == "bytestream" {
				v.hasBytestreamTestActionOutputs = true

				// To protect our backends from thrashing -- stop
				// copying outputs if there are way too many. This can
				// happen if a ruleset is buggy.
				if len(v.testOutputURIs) >= maxPersistableArtifacts {
					continue
				}
				v.testOutputURIs = append(v.testOutputURIs, u)
			}
		}
	}
	return nil
}

// SetExpectedMetadataEvents sets the list of metadata event IDs that are
// expected in this build.
func (v *BEValues) SetExpectedMetadataEvents(events []*build_event_stream.BuildEventId) {
	for _, childEventID := range events {
		if IsMetadataEvent(childEventID) {
			v.unprocessedMetadataEvents[childEventID.String()] = struct{}{}
		}
	}
}

func (v *BEValues) MetadataIsLoaded() bool {
	return v.sawStartedEvent && len(v.unprocessedMetadataEvents) == 0
}

func (v *BEValues) Finalize(ctx context.Context) {
	invocation := v.Invocation()
	invocation.InvocationStatus = inspb.InvocationStatus_DISCONNECTED_INVOCATION_STATUS
	if v.BuildFinished() {
		invocation.InvocationStatus = inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS
	}
}
func (v *BEValues) StartTime() time.Time {
	return v.buildStartTime
}

func (v *BEValues) OutputFiles() map[string]*build_event_stream.File {
	return v.outputFilesMap
}

func (v *BEValues) KytheSSTableResourceName() *rspb.ResourceName {
	return v.kytheSSTableResourceName
}

func (v *BEValues) DisableCommitStatusReporting() bool {
	return v.getBoolValue(disableCommitStatusReportingFieldName)
}

func (v *BEValues) DisableTargetTracking() bool {
	return v.getBoolValue(disableTargetTrackingFieldName)
}

func (v *BEValues) Pattern() string {
	return invocation_format.ShortFormatPatterns(v.Invocation().GetPattern())
}

func (v *BEValues) WorkflowID() string {
	return v.getStringValue(workflowIDFieldName)
}

func (v *BEValues) ActionName() string {
	return v.getStringValue(actionNameFieldName)
}

func (v *BEValues) BuildFinished() bool {
	return v.sawFinishedEvent
}

func (v *BEValues) BuildToolLogURIs() []*url.URL {
	return v.buildToolLogURIs
}

func (v *BEValues) HasBytestreamTestActionOutputs() bool {
	return v.hasBytestreamTestActionOutputs
}

func (v *BEValues) TestOutputURIs() []*url.URL {
	return v.testOutputURIs
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

func (v *BEValues) getBoolValue(fieldName string) bool {
	val := v.getStringValue(fieldName)
	return val == "true" || val == "True" || val == "TRUE" || val == "yes" || val == "1"
}

func (v *BEValues) handleStartedEvent(event *build_event_stream.BuildEvent) {
	v.sawStartedEvent = true
	v.buildStartTime = timeutil.GetTimeWithFallback(event.GetStarted().GetStartTime(), event.GetStarted().GetStartTimeMillis())
}

func (v *BEValues) populateWorkspaceInfoFromBuildMetadata(metadata *build_event_stream.BuildMetadata) {
	for mdKey, mdVal := range metadata.Metadata {
		if fieldName := buildMetadataFieldMapping[mdKey]; fieldName != "" {
			v.setStringValue(fieldName, mdVal)
		}
	}
}

func (v *BEValues) handleWorkflowConfigured(wfc *build_event_stream.WorkflowConfigured) {
	v.setStringValue(workflowIDFieldName, wfc.GetWorkflowId())
	v.setStringValue(actionNameFieldName, wfc.GetActionName())
}

// IsMetadataEvent returns true for events containing invocation-level metadata,
// like repo, commit SHA, etc.
func IsMetadataEvent(eventID *build_event_stream.BuildEventId) bool {
	switch eventID.GetId().(type) {
	case *build_event_stream.BuildEventId_OptionsParsed:
		return true
	case *build_event_stream.BuildEventId_WorkspaceStatus:
		return true
	case *build_event_stream.BuildEventId_BuildMetadata:
		return true
	case *build_event_stream.BuildEventId_StructuredCommandLine:
		return true
	case *build_event_stream.BuildEventId_UnstructuredCommandLine:
		return true
	case *build_event_stream.BuildEventId_WorkflowConfigured:
		return true
	default:
		return false
	}
}

// IsImportantEvent returns true for events that are non-skippable.
// Events are usually not skipped, but when processing extra-large invocations,
// non-important events may be dropped to conserve resources.
func IsImportantEvent(event *build_event_stream.BuildEvent) bool {
	// All events that contain invocation-level metadata are important.
	if IsMetadataEvent(event.GetId()) {
		return true
	}
	switch event.Payload.(type) {
	case *build_event_stream.BuildEvent_Started:
		return true
	case *build_event_stream.BuildEvent_BuildToolLogs:
		return true
	case *build_event_stream.BuildEvent_Finished:
		return true
	default:
		return false
	}
}
