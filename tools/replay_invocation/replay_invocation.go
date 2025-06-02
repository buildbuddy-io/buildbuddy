package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	espb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
)

var (
	besBackend    = flag.String("bes_backend", "", "The bes backend to replay events to.")
	besResultsURL = flag.String("bes_results_url", "", "The invocation URL prefix")
	apiKey        = flag.String("api_key", "", "The API key of the account that will own the replayed events")

	jobs = flag.Int("jobs", 100, "Number of jobs to run.")
)

func getUUID() string {
	u, err := uuid.NewRandom()
	if err != nil {
		log.Fatalf("Error making UUID: %s", err.Error())
	}
	return u.String()
}

func sendInvocation(ctx context.Context, data string) {
	conn, err := grpc_client.DialSimple(*besBackend)
	if err != nil {
		log.Fatalf("Error dialing bes backend: %s", err.Error())
	}
	defer conn.Close()
	client := pepb.NewPublishBuildEventClient(conn)

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	stream, err := client.PublishBuildToolEventStream(ctx)
	if err != nil {
		log.Warningf("Error opening stream: %s", err.Error())
		return
	}
	streamID := &bepb.StreamId{
		InvocationId: getUUID(),
		BuildId:      getUUID(),
	}
	invocationURL := *besResultsURL + streamID.GetInvocationId()
	log.Infof("Replaying invocation; results will be available at %s", invocationURL)

	//stream.Send(&pepb.PublishBuildToolEventStreamRequest{
	//	OrderedBuildEvent: &pepb.OrderedBuildEvent{
	//		Event:
	//	}
	//})

	progress := &espb.Progress{
		// Only outputting to stderr for now, like Bazel does.
		Stderr: data,
	}
	ev := &espb.BuildEvent{
		Payload: &espb.BuildEvent_Progress{Progress: progress},
	}

	for i := 0; i < 10; i++ {
		log.Infof("Sending build event %d\n", i)
		count := int32(i)

		ev.Id = &espb.BuildEventId{Id: &espb.BuildEventId_Progress{Progress: &espb.BuildEventId_ProgressId{OpaqueCount: count}}}
		ev.Children = []*espb.BuildEventId{
			{Id: &espb.BuildEventId_Progress{Progress: &espb.BuildEventId_ProgressId{OpaqueCount: count + 1}}},
		}

		//ev := &espb.BuildEvent{
		//	Id: &espb.BuildEventId{Id: &espb.BuildEventId_Progress{Progress: &espb.BuildEventId_ProgressId{OpaqueCount: count}}},
		//	Children: []*espb.BuildEventId{
		//		{Id: &espb.BuildEventId_Progress{Progress: &espb.BuildEventId_ProgressId{OpaqueCount: count + 1}}},
		//	},
		//	Payload: &espb.BuildEvent_Progress{Progress: &espb.Progress{
		//		// Only outputting to stderr for now, like Bazel does.
		//		Stderr: data,
		//	}},
		//}

		//var in proto.Message
		//if i == 0 {
		//	in = &espb.Progress{}
		//} else {
		//	in = progress
		//}
		//in := progress

		bazelEventAny, err := anypb.New(ev)
		if err != nil {
			log.Warningf("Error marshaling event: %s", err.Error())
			return
		}
		be := &bepb.BuildEvent{
			EventTime: timestamppb.Now(),
			Event:     &bepb.BuildEvent_BazelEvent{BazelEvent: bazelEventAny},
		}
		obe := &pepb.OrderedBuildEvent{
			StreamId:       streamID,
			SequenceNumber: int64(count + 1),
			Event:          be,
		}
		req := &pepb.PublishBuildToolEventStreamRequest{OrderedBuildEvent: obe}
		if err := stream.Send(req); err != nil {
			log.Warningf("Error sending stream: %s", err.Error())
			return
		}
	}
	stream.CloseSend()
	log.Infof("Waiting for response")
	res, err := stream.Recv()
	if err != nil {
		log.Warningf("Error receiving stream: %s", err.Error())
		return
	}
	log.Infof("Got response: %+v", res)
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

	if *apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", *apiKey)
	}

	var dw bytes.Buffer
	for dw.Len() < 40_000_000 {
		s, _ := random.RandomString(16)
		dw.WriteString(fmt.Sprintf("hello world, what a beautiful beautiful day to write some code %s\n", s))
	}
	data := dw.String()

	for i := 0; i < *jobs; i++ {
		go func() {
			for {
				sendInvocation(ctx, data)
			}
		}()
	}
	select {}
}
