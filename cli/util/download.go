package util

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
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"google.golang.org/grpc/metadata"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

type InvocationFileSelector func(inv *inpb.Invocation) *bespb.File

func OpenInvocationFile(invocationID, target, description string, selector InvocationFileSelector) (io.ReadCloser, error) {
	apiKey, err := login.GetAPIKey()
	if err != nil {
		return nil, err
	}
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-buildbuddy-api-key", apiKey)
	backend, err := resolveTarget(target)
	if err != nil {
		return nil, err
	}
	conn, err := grpc_client.DialSimple(backend)
	if err != nil {
		return nil, err
	}
	resource, err := getInvocationResource(ctx, conn, invocationID, description, selector)
	if err != nil {
		conn.Close()
		return nil, err
	}

	bsClient := bspb.NewByteStreamClient(conn)
	// Avoid reading the entire log into memory at once.
	in, out := io.Pipe()
	go func() {
		defer conn.Close()
		err := cachetools.GetBlob(ctx, bsClient, resource, out)
		if err != nil {
			out.CloseWithError(fmt.Errorf("failed to download %s %s for invocation %s: %v", description, resource.DownloadString(), invocationID, err))
		} else {
			out.Close()
		}
	}()
	return in, nil
}

func resolveTarget(target string) (string, error) {
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

func getInvocationResource(ctx context.Context, conn *grpc_client.ClientConnPool, invocationID, description string, selector InvocationFileSelector) (*digest.CASResourceName, error) {
	resp, err := bbspb.NewBuildBuddyServiceClient(conn).GetInvocation(ctx, &inpb.GetInvocationRequest{
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
