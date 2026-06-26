package explain

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"google.golang.org/grpc/metadata"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const timingProfileUsage = `
usage: bb explain timing-profile [--target API_TARGET] {INVOCATION_ID | INVOCATION_URL}

Examples:
  bb explain timing-profile 5e4e42d1-f545-4a21-8135-0e308d9f247a
  bb explain timing-profile https://app.buildbuddy.io/invocation/5e4e42d1-f545-4a21-8135-0e308d9f247a

Analyzes the timing profile for the given invocation.
`

var (
	timingProfileFlags = flag.NewFlagSet("timing-profile", flag.ContinueOnError)
)

func handleTimingProfile(args []string) (int, error) {
	if err := arg.ParseFlagSet(timingProfileFlags, args); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			log.Printf("Failed to parse flags: %s", err)
		}
		log.Print(timingProfileUsage)
		return 1, nil
	}
	if len(timingProfileFlags.Args()) != 1 {
		log.Print(timingProfileUsage)
		return 1, nil
	}
	invocationID := timingProfileFlags.Args()[0]
	return analyzeTimingProfile(invocationID)
}

func analyzeTimingProfile(invocationIDOrURL string) (int, error) {
	invocationID := invocationIDOrURL
	if matches := uuidPattern.FindStringSubmatch(invocationIDOrURL); matches != nil {
		invocationID = matches[1]
	}

	profile, err := openTimingProfile(invocationID)
	if err != nil {
		return -1, err
	}
	defer profile.Close()

	// TODO: Analyze profile.
	return 0, nil
}

func openTimingProfile(invocationID string) (io.ReadCloser, error) {
	apiKey, err := login.GetAPIKey()
	if err != nil {
		return nil, err
	}
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-buildbuddy-api-key", apiKey)
	conn, err := grpc_client.DialSimple(*apiTarget)
	if err != nil {
		return nil, err
	}
	resource, err := getTimingProfileResource(ctx, conn, invocationID)
	if err != nil {
		conn.Close()
		return nil, err
	}

	bsClient := bspb.NewByteStreamClient(conn)
	in, out := io.Pipe()
	go func() {
		defer conn.Close()
		err := cachetools.GetBlob(ctx, bsClient, resource, out)
		if err != nil {
			out.CloseWithError(fmt.Errorf("failed to download timing profile %s for invocation %s: %v", resource.DownloadString(), invocationID, err))
		} else {
			out.Close()
		}
	}()
	return in, nil
}

func getTimingProfileResource(ctx context.Context, conn *grpc_client.ClientConnPool, invocationID string) (*digest.CASResourceName, error) {
	resp, err := bbspb.NewBuildBuddyServiceClient(conn).GetInvocation(ctx, &inpb.GetInvocationRequest{
		Lookup: &inpb.InvocationLookup{InvocationId: invocationID},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch invocation %s: %v", invocationID, err)
	}
	if len(resp.GetInvocation()) == 0 {
		return nil, fmt.Errorf("no such invocation: %s", invocationID)
	}
	inv := resp.GetInvocation()[0]
	file := findTimingProfileLog(inv)
	if file == nil {
		return nil, fmt.Errorf("no timing profile found for invocation %s", invocationID)
	}
	if !strings.HasPrefix(file.GetUri(), "bytestream://") {
		return nil, fmt.Errorf("unsupported timing profile URI for %q: %s", file.GetName(), file.GetUri())
	}
	bytestreamURL, err := url.Parse(file.GetUri())
	if err != nil {
		return nil, fmt.Errorf("failed to parse timing profile URI: %v", err)
	}
	resource, err := digest.ParseDownloadResourceName(bytestreamURL.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to parse timing profile resource: %v", err)
	}
	return resource, nil
}

func findTimingProfileLog(inv *inpb.Invocation) *bespb.File {
	for _, event := range inv.GetEvent() {
		for _, logFile := range event.GetBuildEvent().GetBuildToolLogs().GetLog() {
			if logFile.GetUri() != "" && strings.HasPrefix(logFile.GetName(), "command.profile.") {
				return logFile
			}
		}
	}
	return nil
}
