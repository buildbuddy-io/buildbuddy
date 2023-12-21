package main

import (
	"context"
	"flag"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
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
)

var (
	invocationID  = flag.String("invocation_id", "", "The invocation ID to replay.")
	besBackend    = flag.String("bes_backend", "", "The bes backend to replay events to.")
	besResultsURL = flag.String("bes_results_url", "", "The invocation URL prefix")
	apiKey        = flag.String("api_key", "", "The API key of the account that will own the replayed events")
	// TODO: Figure out the latest attempt number automatically.
	attemptNumber = flag.Int("attempt", 1, "Invocation attempt number.")

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
				err := stream.CloseSend()
				if err != nil {
					log.Fatalf("Error closing stream: %s", err.Error())
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
