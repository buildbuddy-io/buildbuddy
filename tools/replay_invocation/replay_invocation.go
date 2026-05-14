package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"regexp"
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

	copyArtifacts = flag.Bool("copy_artifacts", false, "Copy blobstore-persisted invocation artifacts to the cache target. This is required to view test logs, timing profile, and other files in the build event stream.")

	metadataOverride arrayFlags

	// Note: you will also need to configure a blobstore.

	apiKeyRegex = regexp.MustCompile(`x-buildbuddy-api-key=([[:alnum:]]+)`)
)

func init() {
	flag.Var(&metadataOverride, "metadata_override", "Array of build metadata values to override")
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
			if *apiKey != "" {
				// Overwrite API key in the started event options with the one set via
				// flag.
				p.Started.OptionsDescription = apiKeyRegex.ReplaceAllLiteralString(
					p.Started.OptionsDescription,
					"x-buildbuddy-api-key="+*apiKey,
				)
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

		a := &anypb.Any{}
		if err := a.MarshalFrom(buildEvent); err != nil {
			log.Fatalf("Error marshaling bazel event to any: %s", err.Error())
		}
		req := pepb.PublishBuildToolEventStreamRequest{
			OrderedBuildEvent: &pepb.OrderedBuildEvent{
				StreamId:       streamID,
				SequenceNumber: sequenceNum,
				Event: &bepb.BuildEvent{
					EventTime: ie.GetEventTime(),
					Event:     &bepb.BuildEvent_BazelEvent{BazelEvent: a},
				},
			},
		}
		if err := stream.Send(&req); err != nil {
			log.Fatalf("Error sending event on stream: %s", err.Error())
		}
	}

	// Fetch invocation log chunks from the original invocation and replay them
	// as synthetic progress events. Note: if we're using a
	// build_event_json_file, we have the original progress events and can
	// replay them directly.
	if *buildEventJSONFile == "" {
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
	rn, err := digest.ParseDownloadResourceName(parsedURL.Path)
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
