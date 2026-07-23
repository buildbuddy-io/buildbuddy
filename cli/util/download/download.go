package download

import (
	"context"
	"fmt"
	"io"
	"net/http"
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

type Downloader interface {
	GetBytestreamFile(ctx context.Context, uri string, w io.Writer) error
}

type byteStreamDownloader struct {
	bsClient bspb.ByteStreamClient
}

func NewByteStreamDownloader(bsClient bspb.ByteStreamClient) Downloader {
	return &byteStreamDownloader{bsClient: bsClient}
}

// GetInvocationFile downloads an invocation file.
// The /file/download endpoint first reads the file
// from CAS, then falls back to the invocation's blobstore copy if the CAS entry
// is no longer available.
//
// Pass an io.PipeWriter to stream the contents to a
// reader without buffering the entire file in memory.
func GetInvocationFile(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, w io.Writer, appURL, invocationID, description string, selector InvocationFileSelector) error {
	apiKey, err := login.GetAPIKey()
	if err != nil {
		return err
	}

	// Fetch the URI for the file from the invocation.
	grpcCtx := metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey)
	file, err := getInvocationResource(grpcCtx, bbClient, invocationID, description, selector)
	if err != nil {
		return err
	}

	// Download the file from the URI.
	downloadURL, err := fileDownloadURL(appURL, file.GetUri(), invocationID)
	if err != nil {
		return fmt.Errorf("create %s download URL: %w", description, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
	if err != nil {
		return fmt.Errorf("create %s download request: %w", description, err)
	}
	req.Header.Set("x-buildbuddy-api-key", apiKey)
	// Preserve the bytes as stored. In particular, timing profiles ending in
	// .gz should remain gzipped on disk rather than being transparently decoded
	// by net/http.
	req.Header.Set("Accept-Encoding", "identity")

	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to download %s for invocation %s: %w", description, invocationID, err)
	}
	defer rsp.Body.Close()
	if rsp.StatusCode < 200 || rsp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(rsp.Body, 4<<10))
		message := strings.TrimSpace(string(body))
		if message == "" {
			message = rsp.Status
		}
		return fmt.Errorf("failed to download %s for invocation %s: %s", description, invocationID, message)
	}
	if _, err := io.Copy(w, rsp.Body); err != nil {
		return fmt.Errorf("failed to download %s for invocation %s: %w", description, invocationID, err)
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

func getInvocationResource(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, invocationID, description string, selector InvocationFileSelector) (*bespb.File, error) {
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
	return file, nil
}

func fileDownloadURL(appURL, bytestreamURL, invocationID string) (string, error) {
	u, err := url.Parse(appURL)
	if err != nil {
		return "", err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("invalid BuildBuddy web URL %q", appURL)
	}
	u.Path = "/file/download"
	u.RawPath = ""
	u.RawQuery = url.Values{
		"bytestream_url": {bytestreamURL},
		"invocation_id":  {invocationID},
	}.Encode()
	u.Fragment = ""
	return u.String(), nil
}

func (d *byteStreamDownloader) GetBytestreamFile(ctx context.Context, uri string, w io.Writer) error {
	return GetBytestreamFile(ctx, d.bsClient, uri, w)
}

// GetBytestreamFile downloads the contents of a bytestream:// URI.
func GetBytestreamFile(ctx context.Context, bsClient bspb.ByteStreamClient, uri string, w io.Writer) error {
	resource, err := parseBytestreamURI(uri)
	if err != nil {
		return err
	}
	if err := cachetools.GetBlob(ctx, bsClient, resource, w); err != nil {
		return fmt.Errorf("failed to download %s: %w", resource.DownloadString(), err)
	}
	return nil
}

// parseBytestreamURI parses a bytestream:// URI into a CAS resource name.
func parseBytestreamURI(uri string) (*digest.CASResourceName, error) {
	if !strings.HasPrefix(uri, "bytestream://") {
		return nil, fmt.Errorf("unsupported bytestream URI: %s", uri)
	}
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse bytestream URI: %w", err)
	}
	return digest.ParseDownloadResourceName(u.Path)
}
