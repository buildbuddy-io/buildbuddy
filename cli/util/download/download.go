package download

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/flaghistory"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"google.golang.org/grpc/metadata"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

type InvocationFileSelector func(inv *inpb.Invocation) *bespb.File

// GetInvocationFile downloads the specified file from the given
// invocation and writes it to w. Pass an io.PipeWriter to stream the contents
// to a reader without buffering the entire file in memory.
func GetInvocationFile(ctx context.Context, bsClient bspb.ByteStreamClient, bbClient bbspb.BuildBuddyServiceClient, w io.Writer, invocationID, description string, selector InvocationFileSelector) error {
	apiKey, err := login.GetAPIKey()
	if err != nil {
		return err
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey)
	resource, err := getInvocationResource(ctx, bbClient, invocationID, description, selector)
	if err != nil {
		return err
	}

	if err := cachetools.GetBlob(ctx, bsClient, resource, w); err != nil {
		return fmt.Errorf("failed to download %s %s for invocation %s: %v", description, resource.DownloadString(), invocationID, err)
	}
	return nil
}

func ResolveTarget(target string) (string, error) {
	if target != "" {
		return target, nil
	}
	backend, err := flaghistory.GetLastBackend()
	if err != nil {
		log.Debugf("Failed to get last backend: %v", err)
	}
	if backend == "" {
		backend = login.DefaultApiTarget
	}
	return backend, nil
}

func getInvocationResource(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, invocationID, description string, selector InvocationFileSelector) (*digest.CASResourceName, error) {
	resp, err := bbClient.GetInvocation(ctx, &inpb.GetInvocationRequest{
		Lookup: &inpb.InvocationLookup{InvocationId: invocationID},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch invocation %s: %v", invocationID, err)
	}
	if len(resp.GetInvocation()) == 0 {
		return nil, fmt.Errorf("no such invocation: %s", invocationID)
	}
	file := selector(resp.GetInvocation()[0])
	if file == nil {
		return nil, fmt.Errorf("no %s found for invocation %s", description, invocationID)
	}
	if !strings.HasPrefix(file.GetUri(), "bytestream://") {
		return nil, fmt.Errorf("unsupported %s URI for %q: %s", description, file.GetName(), file.GetUri())
	}
	bytestreamURL, err := url.Parse(file.GetUri())
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s URI: %v", description, err)
	}
	resource, err := digest.ParseDownloadResourceName(bytestreamURL.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s resource: %v", description, err)
	}
	return resource, nil
}
