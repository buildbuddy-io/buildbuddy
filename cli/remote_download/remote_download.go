package remote_download

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
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

The resource can be downloaded using 'bb download':

	bb remote-download <url> | xargs bb download

The "checksum.sri" qualifier can be used to specify a checksum.
Example:

	SHA256=$(curl -fsSL https://example.com | sha256sum | awk '{print $1}')
	SRI="sha256-$(echo "$SHA256" | xxd -r -p | base64 -w 0)"
	bb remote-download --qualifier=checksum.sri="$SRI" https://example.com
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
	for _, q := range *qualifiers {
		name, value, ok := strings.Cut(q, "=")
		if !ok {
			return -1, fmt.Errorf("invalid qualifier (expected NAME=VALUE)")
		}
		req.Qualifiers = append(req.Qualifiers, &rapb.Qualifier{
			Name:  name,
			Value: value,
		})
		fmt.Printf("Qualifier name: %q value: %q\n", name, value)
	}
	client := rapb.NewFetchClient(conn)

	apiKey, err := login.GetAPIKeyInteractively()
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
