package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"

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
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/anypb"

	espb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	invocationID  = flag.String("invocation_id", "", "The invocation ID to replay.")
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

	env := real_environment.NewRealEnv(healthcheck.NewHealthChecker(""))
	ctx := env.GetServerContext()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	env.GetHealthChecker().RegisterShutdownFunction(func(_ context.Context) error {
		cancel()
		return nil
	})

	bs, err := blobstore.GetConfiguredBlobstore(env)
	if err != nil {
		log.Fatalf("Error configuring blobstore: %s", err.Error())
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
	protoStreamID := build_event_handler.GetStreamIdFromInvocationIdAndAttempt(*invocationID, uint64(*attemptNumber))
	eventAllocator := func() proto.Message { return &inpb.InvocationEvent{} }
	pr := protofile.NewBufferedProtoReader(bs, protoStreamID, eventAllocator)
	sequenceNum := int64(0)
	streamID := &bepb.StreamId{
		InvocationId: getUUID(),
		BuildId:      getUUID(),
	}
	invocationURL := *besResultsURL + streamID.GetInvocationId()
	log.Infof("Replaying invocation; results will be available at %s", invocationURL)
	for {
		msg, err := pr.ReadProto(ctx)
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
		ie := msg.(*inpb.InvocationEvent)
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

	// Fetch invocation log chunks and replay them as synthetic progress
	// events.
	logsBlobstorePrefix := eventlog.GetEventLogPathFromInvocationIdAndAttempt(*invocationID, uint64(*attemptNumber))
	log.Infof("Fetching log chunks from %s_*", logsBlobstorePrefix)
	reader := chunkstore.New(bs, &chunkstore.ChunkstoreOptions{}).Reader(ctx, logsBlobstorePrefix)
	buf := make([]byte, 4096)
	for i := 0; ; i++ {
		n, err := reader.Read(buf)

		if n > 0 {
			b := buf[:n]
			if *printLogs {
				os.Stderr.Write(b)
			}

			sequenceNum += 1
			a := &anypb.Any{}
			buildEvent := &espb.BuildEvent{
				Id: &espb.BuildEventId{Id: &espb.BuildEventId_Progress{}},
				Payload: &espb.BuildEvent_Progress{Progress: &espb.Progress{
					Stderr: string(b),
				}},
			}
			if err := a.MarshalFrom(buildEvent); err != nil {
				log.Fatalf("Error marshaling bazel event to any: %s", err.Error())
			}
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

		if err == io.EOF {
			break
		}
		if err != nil {
			log.Errorf("Failed to read log chunks: %s", err)
			break
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
