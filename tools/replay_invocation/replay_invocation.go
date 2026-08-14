package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore"
	"github.com/buildbuddy-io/buildbuddy/server/backends/chunkstore"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/accumulator"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/eventlog"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/protofile"
	"github.com/buildbuddy-io/buildbuddy/server/util/redact"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	espb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	// Event source: can either be an invocation_id or build_event_json_file.
	invocationID       = flag.String("invocation_id", "", "The invocation ID to replay.")
	buildEventJSONFile = flag.String("build_event_json_file", "", "If set, replay from a build_event_json_file instead of from the original invocation ID.")
	rawJSONFile        = flag.String("raw_json_file", "", "If set, replay from a json file downloaded from the Raw tab of BuildBuddy Web UI.")

	besBackend    = flag.String("bes_backend", "", "The bes backend to replay events to.")
	besResultsURL = flag.String("bes_results_url", "", "The invocation URL prefix")
	cacheTarget   = flag.String("cache_target", "", "Cache target where artifacts are copied, if applicable. Defaults to bes_backend.")
	apiKey        = flag.String("api_key", "", "The API key of the account that will own the replayed events")
	printLogs     = flag.Bool("print_logs", false, "Copy logs from Progress events to stdout/stderr.")
	// TODO: Figure out the latest attempt number automatically.
	attemptNumber = flag.Int("attempt", 1, "Invocation attempt number.")

	copyArtifacts              = flag.Bool("copy_artifacts", false, "Copy blobstore-persisted invocation artifacts to the cache target. This is required to view test logs, timing profile, and other files in the build event stream.")
	copyErrorTrackingArtifacts = flag.Bool("copy_error_tracking_artifacts", false, "Copy only URI-backed failed-test and failed-action diagnostics from a remote invocation into the destination cache.")
	sourceBaseURL              = flag.String("source_base_url", "", "HTTP base URL used to fetch remote error-tracking artifacts, such as https://buildbuddy.buildbuddy.io.")
	sourceInvocationID         = flag.String("source_invocation_id", "", "Remote invocation ID used to authorize artifact downloads when replaying a raw JSON file.")
	sourceAPIKeyFile           = flag.String("source_api_key_file", "", "Mode-0600 file containing the source server API key. The key is never passed on the command line.")
	destinationArtifactHost    = flag.String("destination_artifact_host", "", "Host to write into copied bytestream URIs; must match the destination server cache_api_url host.")
	eventTimeUsec              = flag.Int64("event_time_usec", 0, "If positive, use this timestamp for every replayed event. Useful for preserving approximate invocation chronology from raw JSON exports.")

	metadataOverride    arrayFlags
	sourceArtifactHosts arrayFlags

	// Note: you will also need to configure a blobstore.

	apiKeyRegex = regexp.MustCompile(`(?i)x-buildbuddy-api-key(?:=|\s+)[^\s'\"]+`)
)

const (
	maxImportedActionOutputBytes  = 4 << 10
	maxImportedTestLogBytes       = 4 << 10
	maxImportedTestXMLBytes       = 1 << 20
	maxImportedArtifactBytes      = 8 << 20
	maxImportedRedactionLookahead = 64 << 10
	maxImportedArtifactCandidates = 1024
	maxImportedArtifactRequests   = 256
)

func init() {
	flag.Var(&metadataOverride, "metadata_override", "Array of build metadata values to override")
	flag.Var(&sourceArtifactHosts, "source_artifact_host", "Allowed bytestream artifact host. Set once per expected source cache host.")
}

type arrayFlags []string

func (i *arrayFlags) String() string {
	return "An array of strings -- set multiple!"
}
func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func getUUID() string {
	u, err := uuid.NewRandom()
	if err != nil {
		log.Fatalf("Error making UUID: %s", err.Error())
	}
	return u.String()
}

type errorTrackingArtifactCandidate struct {
	file        *espb.File
	maxBytes    int64
	mayTruncate bool
}

func isRemoteBytestreamFile(file *espb.File) bool {
	u, err := url.Parse(file.GetUri())
	return err == nil && u.Scheme == "bytestream" && u.Host != ""
}

func errorTrackingArtifactCandidates(event *espb.BuildEvent) []errorTrackingArtifactCandidate {
	return slices.DeleteFunc(errorTrackingArtifactCandidatesIncludingInline(event), func(candidate errorTrackingArtifactCandidate) bool {
		return !isRemoteBytestreamFile(candidate.file)
	})
}

func errorTrackingArtifactCandidatesIncludingInline(event *espb.BuildEvent) []errorTrackingArtifactCandidate {
	var candidates []errorTrackingArtifactCandidate
	switch payload := event.GetPayload().(type) {
	case *espb.BuildEvent_Action:
		if payload.Action.GetSuccess() {
			return nil
		}
		for _, file := range []*espb.File{payload.Action.GetStderr(), payload.Action.GetStdout()} {
			if file != nil {
				candidates = append(candidates, errorTrackingArtifactCandidate{
					file: file, maxBytes: maxImportedActionOutputBytes, mayTruncate: true,
				})
			}
		}
	case *espb.BuildEvent_TestResult:
		status := payload.TestResult.GetStatus()
		if status == espb.TestStatus_PASSED || status == espb.TestStatus_FLAKY || status == espb.TestStatus_NO_STATUS {
			return nil
		}
		for _, file := range payload.TestResult.GetTestActionOutput() {
			switch path.Base(file.GetName()) {
			case "test.xml":
				candidates = append(candidates, errorTrackingArtifactCandidate{
					file: file, maxBytes: maxImportedTestXMLBytes,
				})
			case "test.log":
				candidates = append(candidates, errorTrackingArtifactCandidate{
					file: file, maxBytes: maxImportedTestLogBytes, mayTruncate: true,
				})
			}
		}
	}
	return candidates
}

func redactArtifactText(b []byte, sourceAPIKey string, xmlSafe bool) []byte {
	redacted := redact.RedactTextWithValues(string(b), []string{sourceAPIKey})
	if xmlSafe {
		redacted = strings.ReplaceAll(redacted, "<REDACTED>", "[REDACTED]")
	}
	return []byte(redacted)
}

func redactInlineErrorTrackingArtifacts(event *espb.BuildEvent, sourceAPIKey string) {
	for _, candidate := range errorTrackingArtifactCandidatesIncludingInline(event) {
		contents, ok := candidate.file.GetFile().(*espb.File_Contents)
		if !ok {
			continue
		}
		b := contents.Contents
		if int64(len(b)) > candidate.maxBytes {
			if !candidate.mayTruncate {
				contents.Contents = nil
				continue
			}
			b = b[:min(int64(len(b)), candidate.maxBytes+maxImportedRedactionLookahead)]
		}
		b = redactArtifactText(b, sourceAPIKey, path.Base(candidate.file.GetName()) == "test.xml")
		if int64(len(b)) > candidate.maxBytes {
			if !candidate.mayTruncate {
				contents.Contents = nil
				continue
			}
			b = b[:candidate.maxBytes]
		}
		contents.Contents = b
	}
}

func scrubStartedAPIKey(optionsDescription, replacementAPIKey string) string {
	replacement := "x-buildbuddy-api-key=<REDACTED>"
	if replacementAPIKey != "" {
		replacement = "x-buildbuddy-api-key=" + replacementAPIKey
	}
	return apiKeyRegex.ReplaceAllString(optionsDescription, replacement)
}

type errorTrackingArtifactImporter struct {
	client              *http.Client
	sourceBaseURL       *url.URL
	sourceArtifactHosts map[string]struct{}
	sourceInvocationID  string
	sourceAPIKey        string
	destinationHost     string
	upload              func(context.Context, string, repb.DigestFunction_Value, []byte) error
	importedBytes       int64
	consideredArtifacts int
	requestedArtifacts  int
	importedArtifacts   int
	truncatedArtifacts  int
	missingArtifacts    int
	rejectedArtifacts   int
}

func parseSourceBaseURL(rawURL string) (*url.URL, error) {
	baseURL, err := url.Parse(rawURL)
	if err != nil || baseURL.Host == "" || baseURL.User != nil || baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return nil, status.InvalidArgumentError("source_base_url must be an absolute HTTPS URL without credentials, query, or fragment")
	}
	if baseURL.Scheme == "https" {
		return baseURL, nil
	}
	hostname := baseURL.Hostname()
	isLoopback := hostname == "localhost"
	if ip := net.ParseIP(hostname); ip != nil {
		isLoopback = ip.IsLoopback()
	}
	if baseURL.Scheme != "http" || !isLoopback {
		return nil, status.InvalidArgumentError("source_base_url must use HTTPS; HTTP is allowed only for loopback test servers")
	}
	return baseURL, nil
}

func parseSourceArtifactHosts(hosts []string) (map[string]struct{}, error) {
	allowed := make(map[string]struct{}, len(hosts))
	for _, host := range hosts {
		u, err := url.Parse("bytestream://" + host)
		if err != nil || u.Host != host || u.Hostname() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
			return nil, status.InvalidArgumentErrorf("source_artifact_host %q must contain only a host and optional port", host)
		}
		allowed[host] = struct{}{}
	}
	if len(allowed) == 0 {
		return nil, status.InvalidArgumentError("at least one source_artifact_host is required")
	}
	return allowed, nil
}

func newSourceHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 15 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func redactRemoteReplayEvent(ctx context.Context, redactor *redact.StreamingRedactor, sourceAPIKey string, event *espb.BuildEvent) error {
	redactionContext := context.WithValue(ctx, "x-buildbuddy-api-key", sourceAPIKey)
	if err := redactor.RedactAPIKeysWithSlowRegexp(redactionContext, event); err != nil {
		return fmt.Errorf("redact API keys: %w", err)
	}
	if err := redactor.RedactMetadata(event); err != nil {
		return fmt.Errorf("redact metadata: %w", err)
	}
	return nil
}

func newErrorTrackingArtifactImporter(dst bspb.ByteStreamClient) (*errorTrackingArtifactImporter, error) {
	if *sourceBaseURL == "" || *sourceInvocationID == "" || *sourceAPIKeyFile == "" || *destinationArtifactHost == "" {
		return nil, status.InvalidArgumentError("copy_error_tracking_artifacts requires source_base_url, source_invocation_id, source_api_key_file, and destination_artifact_host")
	}
	baseURL, err := parseSourceBaseURL(*sourceBaseURL)
	if err != nil {
		return nil, err
	}
	allowedHosts, err := parseSourceArtifactHosts(sourceArtifactHosts)
	if err != nil {
		return nil, err
	}
	if strings.Contains(*destinationArtifactHost, "://") || strings.ContainsAny(*destinationArtifactHost, "/?#") {
		return nil, status.InvalidArgumentError("destination_artifact_host must contain only a host and optional port")
	}
	info, err := os.Stat(*sourceAPIKeyFile)
	if err != nil {
		return nil, fmt.Errorf("stat source API key file: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, status.PermissionDeniedError("source_api_key_file must not be accessible by group or other users")
	}
	keyBytes, err := os.ReadFile(*sourceAPIKeyFile)
	if err != nil {
		return nil, fmt.Errorf("read source API key file: %w", err)
	}
	key := strings.TrimSpace(string(keyBytes))
	if key == "" {
		return nil, status.InvalidArgumentError("source_api_key_file is empty")
	}
	return &errorTrackingArtifactImporter{
		client:              newSourceHTTPClient(),
		sourceBaseURL:       baseURL,
		sourceArtifactHosts: allowedHosts,
		sourceInvocationID:  *sourceInvocationID,
		sourceAPIKey:        key,
		destinationHost:     *destinationArtifactHost,
		upload: func(ctx context.Context, instanceName string, digestFunction repb.DigestFunction_Value, b []byte) error {
			_, err := cachetools.UploadBlobToCAS(ctx, dst, instanceName, digestFunction, b)
			return err
		},
	}, nil
}

func (i *errorTrackingArtifactImporter) importCandidate(ctx context.Context, candidate errorTrackingArtifactCandidate) error {
	if i.consideredArtifacts >= maxImportedArtifactCandidates {
		i.rejectedArtifacts++
		return status.ResourceExhaustedError("per-invocation artifact candidate budget exhausted")
	}
	i.consideredArtifacts++
	if i.importedBytes >= maxImportedArtifactBytes {
		i.rejectedArtifacts++
		return status.ResourceExhaustedError("per-invocation imported artifact budget exhausted")
	}
	parsedURI, err := url.Parse(candidate.file.GetUri())
	if err != nil || parsedURI.Scheme != "bytestream" || parsedURI.Host == "" {
		i.rejectedArtifacts++
		return status.InvalidArgumentError("artifact is not an absolute bytestream URI")
	}
	if _, ok := i.sourceArtifactHosts[parsedURI.Host]; !ok {
		i.rejectedArtifacts++
		return status.PermissionDeniedErrorf("artifact host %q is not an allowed source_artifact_host", parsedURI.Host)
	}
	originalResource, err := digest.ParseDownloadResourceName(parsedURI.Path)
	if err != nil {
		i.rejectedArtifacts++
		return fmt.Errorf("parse artifact resource name: %w", err)
	}
	if originalResource.GetCompressor() != repb.Compressor_IDENTITY {
		i.rejectedArtifacts++
		return status.UnimplementedError("compressed source artifacts are not supported")
	}
	limit := min(candidate.maxBytes, int64(maxImportedArtifactBytes)-i.importedBytes)
	if limit <= 0 {
		i.rejectedArtifacts++
		return status.ResourceExhaustedError("per-invocation imported artifact budget exhausted")
	}
	if !candidate.mayTruncate && originalResource.GetDigest().GetSizeBytes() > limit {
		i.rejectedArtifacts++
		return status.ResourceExhaustedError("artifact exceeds import limit")
	}
	downloadURL := *i.sourceBaseURL
	downloadURL.Path = "/file/download"
	query := downloadURL.Query()
	query.Set("invocation_id", i.sourceInvocationID)
	query.Set("bytestream_url", candidate.file.GetUri())
	downloadURL.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("x-buildbuddy-api-key", i.sourceAPIKey)
	if i.requestedArtifacts >= maxImportedArtifactRequests {
		i.rejectedArtifacts++
		return status.ResourceExhaustedError("per-invocation artifact request budget exhausted")
	}
	i.requestedArtifacts++
	response, err := i.client.Do(req)
	if err != nil {
		i.missingArtifacts++
		return fmt.Errorf("download source artifact: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		i.missingArtifacts++
		return status.UnavailableErrorf("source artifact download returned HTTP %d", response.StatusCode)
	}
	readLimit := limit + maxImportedRedactionLookahead
	b, err := io.ReadAll(io.LimitReader(response.Body, readLimit+1))
	if err != nil {
		return fmt.Errorf("read source artifact: %w", err)
	}
	responseOverflow := int64(len(b)) > readLimit
	if responseOverflow {
		b = b[:readLimit]
	}
	declaredSize := originalResource.GetDigest().GetSizeBytes()
	if !responseOverflow && int64(len(b)) != declaredSize {
		i.rejectedArtifacts++
		return status.DataLossError("downloaded artifact size does not match its declared digest")
	}
	if responseOverflow && declaredSize <= readLimit {
		i.rejectedArtifacts++
		return status.DataLossError("downloaded artifact exceeds its declared size")
	}
	sourceTruncated := declaredSize > limit
	if sourceTruncated && !candidate.mayTruncate {
		i.rejectedArtifacts++
		return status.ResourceExhaustedError("artifact exceeds import limit")
	}
	if !responseOverflow {
		sourceDigest, err := digest.Compute(bytes.NewReader(b), originalResource.GetDigestFunction())
		if err != nil {
			return fmt.Errorf("compute source artifact digest: %w", err)
		}
		if sourceDigest.GetHash() != originalResource.GetDigest().GetHash() || sourceDigest.GetSizeBytes() != originalResource.GetDigest().GetSizeBytes() {
			i.rejectedArtifacts++
			return status.DataLossError("downloaded artifact does not match its declared digest")
		}
	}
	truncated := sourceTruncated
	b = redactArtifactText(b, i.sourceAPIKey, path.Base(candidate.file.GetName()) == "test.xml")
	if int64(len(b)) > limit {
		if !candidate.mayTruncate {
			i.rejectedArtifacts++
			return status.ResourceExhaustedError("redacted artifact exceeds import limit")
		}
		b = b[:limit]
		truncated = true
	}
	if truncated {
		i.truncatedArtifacts++
	}
	localDigest, err := digest.Compute(bytes.NewReader(b), originalResource.GetDigestFunction())
	if err != nil {
		return fmt.Errorf("compute imported artifact digest: %w", err)
	}
	localResource := digest.NewCASResourceName(localDigest, originalResource.GetInstanceName(), originalResource.GetDigestFunction())
	if err := i.upload(ctx, localResource.GetInstanceName(), localResource.GetDigestFunction(), b); err != nil {
		return fmt.Errorf("upload imported artifact: %w", err)
	}
	localURI := &url.URL{Scheme: "bytestream", Host: i.destinationHost, Path: "/" + localResource.DownloadString()}
	candidate.file.File = &espb.File_Uri{Uri: localURI.String()}
	i.importedArtifacts++
	i.importedBytes += int64(len(b))
	return nil
}

func main() {
	flag.Parse()

	// If running with `bazel run`, cd to the original working directory so that
	// credentials_file path can be resolved correctly.
	if wd := os.Getenv("BUILD_WORKING_DIRECTORY"); wd != "" {
		if err := os.Chdir(wd); err != nil {
			log.Fatal(err.Error())
		}
	}

	if *buildEventJSONFile == "" && *invocationID == "" && *rawJSONFile == "" {
		log.Fatalf("Must provide either invocation_id or build_event_json_file or raw_json_file")
	}
	sourceFlagCount := 0
	if *buildEventJSONFile != "" {
		sourceFlagCount++
	}
	if *invocationID != "" {
		sourceFlagCount++
	}
	if *rawJSONFile != "" {
		sourceFlagCount++
	}
	if sourceFlagCount > 1 {
		log.Fatalf("Cannot set more than one event source flag. Pick one between invocation_id and build_event_json_file and raw_json_file")
	}
	if *copyArtifacts && *copyErrorTrackingArtifacts {
		log.Fatalf("copy_artifacts and copy_error_tracking_artifacts are mutually exclusive")
	}

	env := real_environment.NewRealEnv(healthcheck.NewHealthChecker(""))
	ctx := env.GetServerContext()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	env.GetHealthChecker().RegisterShutdownFunction(func(_ context.Context) error {
		cancel()
		return nil
	})

	bs, err := blobstore.NewFromConfig(ctx)
	if err != nil {
		log.Fatalf("Error configuring blobstore: %s", err.Error())
	}

	var eventSource EventSource
	if *invocationID != "" {
		// Copy blobs from blobstore
		eventSource = NewBlobstoreEventSource(bs, *invocationID, *attemptNumber)
	} else if *buildEventJSONFile != "" {
		eventSource = NewBuildEventJSONFileEventSource(*buildEventJSONFile, false /* isRawFile */)
	} else {
		eventSource = NewBuildEventJSONFileEventSource(*rawJSONFile, true /* isRawFile */)
	}
	conn, err := grpc_client.DialSimple(*besBackend)
	if err != nil {
		log.Fatalf("Error dialing bes backend: %s", err.Error())
	}
	defer conn.Close()
	client := pepb.NewPublishBuildEventClient(conn)

	var cacheConn *grpc_client.ClientConnPool
	if *cacheTarget == "" {
		// Default to bes_backend connection.
		cacheConn = conn
	} else {
		c, err := grpc_client.DialSimple(*cacheTarget)
		if err != nil {
			log.Fatalf("Error dialing cache target: %s", err)
		}
		cacheConn = c
	}
	bytestreamClient := bspb.NewByteStreamClient(cacheConn)
	var errorArtifactImporter *errorTrackingArtifactImporter
	if *copyErrorTrackingArtifacts {
		errorArtifactImporter, err = newErrorTrackingArtifactImporter(bytestreamClient)
		if err != nil {
			log.Fatalf("Configure error-tracking artifact importer: %s", err)
		}
	}
	var replayRedactor *redact.StreamingRedactor
	if errorArtifactImporter != nil {
		replayRedactor = redact.NewStreamingRedactor()
	}
	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	}
	stream, err := client.PublishBuildToolEventStream(ctx)
	if err != nil {
		log.Fatalf("Error opening stream: %s", err.Error())
	}
	sequenceNum := int64(0)
	streamID := &bepb.StreamId{
		InvocationId: getUUID(),
		BuildId:      getUUID(),
	}
	invocationURL := *besResultsURL + streamID.GetInvocationId()
	log.Infof("Replaying invocation; results will be available at %s", invocationURL)
	for {
		ie, err := eventSource.Next(ctx)
		if err != nil {
			if err == io.EOF {
				if sequenceNum == 0 {
					log.Fatalf("No events found for invocation attempt %d. Try --attempt=%d", *attemptNumber, *attemptNumber+1)
				} else {
					log.Infof("Closing stream after %d events!", sequenceNum)
				}
				break
			}
			log.Fatalf("Error reading invocation event from stream: %s", err.Error())
		}
		sequenceNum += 1
		if sequenceNum%10_000 == 0 {
			log.Infof("Progress: replaying event %d", sequenceNum)
		}
		buildEvent := ie.GetBuildEvent()
		switch p := buildEvent.Payload.(type) {
		case *espb.BuildEvent_Progress:
			if *printLogs {
				// Note: these prints most likely will do nothing, since
				// normally we strip progress output and store it more
				// efficiently in a separate blobstore directory.
				io.WriteString(os.Stderr, p.Progress.GetStderr())
				io.WriteString(os.Stdout, p.Progress.GetStdout())
			}
		case *espb.BuildEvent_Started:
			// Never carry source credentials into a replay. If the destination
			// explicitly requires an API key, substitute only that key.
			if replayRedactor == nil {
				p.Started.OptionsDescription = scrubStartedAPIKey(p.Started.OptionsDescription, *apiKey)
			}
		case *espb.BuildEvent_BuildMetadata:
			for _, override := range metadataOverride {
				parts := strings.Split(override, "=")
				if len(parts) != 2 {
					log.Fatalf("override must be of form KEY=VAL")
				}
				p.BuildMetadata.Metadata[parts[0]] = parts[1]
			}
		}
		if replayRedactor != nil {
			if err := redactRemoteReplayEvent(ctx, replayRedactor, errorArtifactImporter.sourceAPIKey, buildEvent); err != nil {
				log.Fatalf("Redact remotely replayed event: %s", err)
			}
			redactInlineErrorTrackingArtifacts(buildEvent, errorArtifactImporter.sourceAPIKey)
		}

		if *copyArtifacts {
			// Use the logic from accumulator just to parse output files from
			// events.
			fileAccumulator := accumulator.NewBEValues(&inpb.Invocation{})
			fileAccumulator.AddEvent(buildEvent)
			// Copy artifacts from the source blobstore to the target cache before
			// publishing the event containing the bytestream URL references.
			for _, f := range fileAccumulator.OutputFiles() {
				if err := copyArtifact(ctx, bytestreamClient, bs, f.GetUri()); err != nil {
					log.Warningf("Failed to copy file %q: %s", f.GetUri(), err)
					continue
				}
				log.Infof("Copied persisted artifact %q", f.GetUri())
			}
		}
		if errorArtifactImporter != nil {
			for _, candidate := range errorTrackingArtifactCandidates(buildEvent) {
				if err := errorArtifactImporter.importCandidate(ctx, candidate); err != nil {
					log.Warningf("Could not import error-tracking artifact %q: %s", candidate.file.GetName(), err)
				}
			}
		}

		a := &anypb.Any{}
		if err := a.MarshalFrom(buildEvent); err != nil {
			log.Fatalf("Error marshaling bazel event to any: %s", err.Error())
		}
		eventTime := ie.GetEventTime()
		if *eventTimeUsec > 0 {
			eventTime = timestamppb.New(time.UnixMicro(*eventTimeUsec))
		}
		req := pepb.PublishBuildToolEventStreamRequest{
			OrderedBuildEvent: &pepb.OrderedBuildEvent{
				StreamId:       streamID,
				SequenceNumber: sequenceNum,
				Event: &bepb.BuildEvent{
					EventTime: eventTime,
					Event:     &bepb.BuildEvent_BazelEvent{BazelEvent: a},
				},
			},
		}
		if err := stream.Send(&req); err != nil {
			log.Fatalf("Error sending event on stream: %s", err.Error())
		}
	}
	if errorArtifactImporter != nil {
		log.Infof(
			"Imported %d error-tracking artifacts (%d bytes, %d truncated, %d missing, %d rejected)",
			errorArtifactImporter.importedArtifacts,
			errorArtifactImporter.importedBytes,
			errorArtifactImporter.truncatedArtifacts,
			errorArtifactImporter.missingArtifacts,
			errorArtifactImporter.rejectedArtifacts,
		)
	}

	// Fetch invocation log chunks from the original invocation and replay them
	// as synthetic progress events. Note: if we're using a
	// build_event_json_file, we have the original progress events and can
	// replay them directly.
	if *invocationID != "" {
		logsBlobstorePrefix := eventlog.GetEventLogPathFromInvocationIdAndAttempt(*invocationID, uint64(*attemptNumber))
		log.Infof("Fetching log chunks from %s_*", logsBlobstorePrefix)
		chunks := chunkstore.New(bs, &chunkstore.ChunkstoreOptions{})
		for i := 0; ; i++ {
			b, err := chunks.ReadChunk(ctx, logsBlobstorePrefix, uint16(i))
			if len(b) > 0 {
				if *printLogs {
					os.Stderr.Write(b)
				}

				a := &anypb.Any{}
				buildEvent := &espb.BuildEvent{
					Id: &espb.BuildEventId{Id: &espb.BuildEventId_Progress{}},
					Payload: &espb.BuildEvent_Progress{Progress: &espb.Progress{
						Stderr: string(b),
					}},
				}
				if err := a.MarshalFrom(buildEvent); err != nil {
					log.Warningf("Error marshaling bazel progress event to any; dropping event: %s", err)
					continue
				}
				sequenceNum += 1
				stream.Send(&pepb.PublishBuildToolEventStreamRequest{
					OrderedBuildEvent: &pepb.OrderedBuildEvent{
						StreamId:       streamID,
						SequenceNumber: sequenceNum,
						Event: &bepb.BuildEvent{
							Event: &bepb.BuildEvent_BazelEvent{BazelEvent: a},
						},
					},
				})
			}

			if status.IsNotFoundError(err) {
				break
			}
			if err != nil {
				log.Errorf("Failed to read log chunks: %s", err)
				break
			}
			log.Infof("Replayed log chunk %d", i)
		}
	}

	if err := stream.CloseSend(); err != nil {
		log.Fatalf("Error closing stream: %s", err.Error())
	}

	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Errorf("Error from BES backend: %s", err)
			break
		}
	}

	log.Infof("Done! Results should be visible at %s", invocationURL)
}

// Copies a persisted artifact from the given blobstore to the destination cache
// target.
func copyArtifact(ctx context.Context, dst bspb.ByteStreamClient, src interfaces.Blobstore, uri string) error {
	parsedURL, err := url.Parse(uri)
	if err != nil {
		return fmt.Errorf("parse bytestream URI as URL: %w", err)
	}
	blobName := path.Join(*invocationID, "artifacts", "cache", parsedURL.Path)
	b, err := src.ReadBlob(ctx, blobName)
	if err != nil {
		return fmt.Errorf("read blob %q: %w", blobName, err)
	}
	rn, err := digest.ParseDownloadResourceName(strings.TrimPrefix(parsedURL.Path, "/"))
	if err != nil {
		return fmt.Errorf("parse bytestream URI as resource name: %w", err)
	}
	if _, err := cachetools.UploadBlobToCAS(ctx, dst, rn.GetInstanceName(), rn.GetDigestFunction(), b); err != nil {
		return fmt.Errorf("upload blob to CAS: %w", err)
	}
	return nil
}

type EventSource interface {
	Next(ctx context.Context) (*inpb.InvocationEvent, error)
}

type BlobstoreEventSource struct {
	bs interfaces.Blobstore
	pr *protofile.BufferedProtoReader
}

func NewBlobstoreEventSource(bs interfaces.Blobstore, invocationID string, attemptNumber int) *BlobstoreEventSource {
	return &BlobstoreEventSource{
		bs: bs,
		pr: protofile.NewBufferedProtoReader(
			bs,
			build_event_handler.GetStreamIdFromInvocationIdAndAttempt(invocationID, uint64(attemptNumber)),
			func() proto.Message { return &inpb.InvocationEvent{} },
		),
	}
}

func (e *BlobstoreEventSource) Next(ctx context.Context) (*inpb.InvocationEvent, error) {
	msg, err := e.pr.ReadProto(ctx)
	if err != nil {
		return nil, err
	}
	return msg.(*inpb.InvocationEvent), nil
}

type BuildEventJSONFileEventSource struct {
	filename       string
	f              *os.File
	s              *bufio.Scanner
	sequenceNumber int64

	isRawFile bool
}

func NewBuildEventJSONFileEventSource(filename string, isRawFile bool) *BuildEventJSONFileEventSource {
	return &BuildEventJSONFileEventSource{filename: filename, isRawFile: isRawFile}
}

func (e *BuildEventJSONFileEventSource) Next(ctx context.Context) (*inpb.InvocationEvent, error) {
	if e.f == nil {
		f, err := os.Open(e.filename)
		if err != nil {
			return nil, fmt.Errorf("open build event JSON file: %w", err)
		}
		e.f = f
		e.s = bufio.NewScanner(f)
		const bufsize = 1024 * 1024 * 10
		e.s.Buffer(make([]byte, bufsize), bufsize)
	}
	// Scan until we either find the next line starting with '{', or we hit EOF,
	// or we hit an error.
	for e.s.Scan() {
		line := e.s.Text()
		if !strings.HasPrefix(line, "{") {
			continue
		}
		if e.isRawFile {
			line = strings.TrimSuffix(line, ",")
		}
		var be espb.BuildEvent
		if err := protojson.Unmarshal([]byte(line), &be); err != nil {
			return nil, fmt.Errorf("unmarshal build event: %w", err)
		}
		e.sequenceNumber++
		return &inpb.InvocationEvent{
			EventTime:      timestamppb.New(time.Now()),
			BuildEvent:     &be,
			SequenceNumber: e.sequenceNumber,
		}, nil
	}
	if err := e.s.Err(); err != nil {
		return nil, fmt.Errorf("scan build event JSON file: %w", err)
	}
	return nil, io.EOF
}
