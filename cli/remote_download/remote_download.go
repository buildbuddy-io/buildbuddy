package remote_download

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/mattn/go-isatty"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"

	rapb "github.com/buildbuddy-io/buildbuddy/proto/remote_asset"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gstatus "google.golang.org/grpc/status"
)

var (
	flags = flag.NewFlagSet("remote-download", flag.ContinueOnError)

	target             = flags.String("target", login.DefaultApiTarget, "Remote gRPC target. Must support bytestream and remote asset APIs.")
	remoteInstanceName = flags.String("remote_instance_name", "", "Remote instance name - typically acts as a cache namespace.")
	digestFunction     = flags.String("digest_function", "", "Digest function specifying how the blob is cached. Must be supported by the remote server.")
	timeout            = flags.Duration("timeout", 0, "Fetch timeout.")
	qualifiers         = flag.New(flags, "qualifier", []string{}, "Qualifiers in NAME=VALUE format.")

	usage = `
usage: bb ` + flags.Name() + ` <url>

Fetches a file via an intermediate cache server and prints the resulting
cache resource name to stdout.

The resource itself can be downloaded using 'bb download':

	bb remote-download <url> | xargs bb download

The "checksum.sri" qualifier can be used to specify a checksum.
If not provided, the URL is always fetched from source, in case the contents
have changed since the last fetch. This may not be desirable if the contents are already cached.

Example:

	SHA256=$(curl -fsSL https://example.com | sha256sum | awk '{print $1}')
	SRI="sha256-$(echo "$SHA256" | xxd -r -p | base64 -w 0)"
	bb remote-download --qualifier=checksum.sri="$SRI" https://example.com

For Google Artifact Registry URLs, the checksum is automatically fetched
from the metadata API if not provided.
`
)

func HandleRemoteDownload(args []string) (int, error) {
	if err := arg.ParseFlagSet(flags, args); err != nil {
		if err == flag.ErrHelp {
			log.Print(usage)
			return 1, nil
		}
		return -1, err
	}

	uris := flags.Args()
	if len(uris) == 0 {
		return -1, fmt.Errorf("missing <url> argument")
	}
	if len(uris) != 1 {
		return -1, fmt.Errorf("only one URL argument is supported")
	}

	parsedDigestFunction := repb.DigestFunction_SHA256
	if *digestFunction != "" {
		df, err := digest.ParseFunction(*digestFunction)
		if err != nil {
			return -1, fmt.Errorf("invalid --digest_function %q", *digestFunction)
		}
		parsedDigestFunction = df
	}

	conn, err := grpc_client.DialSimpleWithoutPooling(*target)
	if err != nil {
		return -1, fmt.Errorf("dial %q: %s", *target, err)
	}
	defer conn.Close()

	ctx := context.Background()

	var timeoutProto *durationpb.Duration
	if *timeout > 0 {
		timeoutProto = durationpb.New(*timeout)
	}
	req := &rapb.FetchBlobRequest{
		InstanceName:   *remoteInstanceName,
		Uris:           uris,
		DigestFunction: parsedDigestFunction,
		Timeout:        timeoutProto,
		// TODO: OldestContentAccepted
	}

	authHeaders := make(map[string]string)
	hasChecksumQualifier := false
	for _, q := range *qualifiers {
		name, value, ok := strings.Cut(q, "=")
		if !ok {
			return -1, fmt.Errorf("invalid qualifier (expected NAME=VALUE)")
		}
		req.Qualifiers = append(req.Qualifiers, &rapb.Qualifier{
			Name:  name,
			Value: value,
		})

		if name == "checksum.sri" {
			hasChecksumQualifier = true
		}

		// Extract Authorization header for metadata fetching
		if strings.HasPrefix(name, "http_header:") {
			headerName := strings.TrimPrefix(name, "http_header:")
			authHeaders[headerName] = value
		}
	}

	// If no checksum qualifier provided, try to fetch it.
	parsedURL, err := url.Parse(uris[0])
	if err != nil {
		return -1, fmt.Errorf("invalid URL: %w", err)
	}
	if !hasChecksumQualifier && isGoogleArtifactRegistryDownloadURL(parsedURL) {
		checksum, err := fetchGoogleArtifactRegistryChecksum(ctx, parsedURL, authHeaders)
		if err != nil {
			log.Warnf("Failed to fetch Artifact Registry checksum. Contents will not be fetched from the cache: %v", err)
		} else if checksum != "" {
			req.Qualifiers = append(req.Qualifiers, &rapb.Qualifier{
				Name:  "checksum.sri",
				Value: checksum,
			})
		}
	}

	client := rapb.NewFetchClient(conn)

	apiKey, err := login.GetAPIKey()
	if err != nil {
		return -1, fmt.Errorf("failed to get API key: %w", err)
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "x-buildbuddy-api-key", apiKey)

	resp, err := client.FetchBlob(ctx, req)
	if err != nil {
		return -1, fmt.Errorf("fetch blob: %w", err)
	}

	if err := gstatus.ErrorProto(resp.GetStatus()); err != nil {
		return -1, err
	}

	rn := digest.NewCASResourceName(resp.GetBlobDigest(), *remoteInstanceName, resp.GetDigestFunction())
	fmt.Print(rn.DownloadString())
	if isatty.IsTerminal(os.Stdout.Fd()) {
		fmt.Println()
	}
	return 0, nil
}

func isGoogleArtifactRegistryDownloadURL(parsedURL *url.URL) bool {
	return parsedURL.Host == "artifactregistry.googleapis.com" && strings.HasSuffix(parsedURL.Path, ":download")
}

// artifactRegistryMetadata is the response from the Google Artifact Registry metadata API
type artifactRegistryMetadata struct {
	Name      string `json:"name"`
	SizeBytes string `json:"sizeBytes"`
	Hashes    []struct {
		Type  string `json:"type"`
		Value string `json:"value"` // base64 encoded
	} `json:"hashes"`
}

// fetchGoogleArtifactRegistryChecksum fetches the checksum for the requested artifact.
//
// The remote asset API takes a checksum qualifier. If not provided, the URL is always fetched from source, in case the contents
// have changed. This may not be desirable if the contents are already cached.
//
// This function returns the checksum in SRI format (EX. "sha256-<base64hash>").
func fetchGoogleArtifactRegistryChecksum(ctx context.Context, googleArtifactRegistryDownloadURL *url.URL, authHeaders map[string]string) (string, error) {
	// Convert download URL to metadata API URL by removing the `:download` suffix.
	metadataURL := *googleArtifactRegistryDownloadURL
	metadataURL.Path = strings.TrimSuffix(googleArtifactRegistryDownloadURL.Path, ":download")
	metadataURL.RawQuery = ""

	req, err := http.NewRequestWithContext(ctx, "GET", metadataURL.String(), nil)
	if err != nil {
		return "", fmt.Errorf("create metadata request: %w", err)
	}

	for key, value := range authHeaders {
		req.Header.Set(key, value)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch metadata: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metadata API returned status %d", resp.StatusCode)
	}

	var metadata artifactRegistryMetadata
	if err := json.NewDecoder(resp.Body).Decode(&metadata); err != nil {
		return "", fmt.Errorf("decode metadata: %w", err)
	}

	for _, hash := range metadata.Hashes {
		if strings.ToUpper(hash.Type) == "SHA256" {
			return "sha256-" + hash.Value, nil
		}
	}

	return "", fmt.Errorf("no SHA256 hash found in metadata")
}
