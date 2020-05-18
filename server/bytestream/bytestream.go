package bytestream

import (
	"context"
	"flag"
	"io"
	"net/url"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	"google.golang.org/grpc"

	bspb "google.golang.org/genproto/googleapis/bytestream"
)

func StreamBytestreamFile(ctx context.Context, uri string, callback func([]byte)) error {
	if uri == "" {
		return status.InvalidArgumentErrorf("File uri must be set")
	}

	if !strings.HasPrefix(uri, "bytestream://") {
		return status.InvalidArgumentErrorf("Only bytestream:// uris are supported")
	}

	parsedURL, err := url.Parse(uri)
	if err != nil {
		return status.InvalidArgumentErrorf("Failed parsing file uri")
	}

	// Optionally, configure HTTP basic-auth.
	dialOptions := make([]grpc.DialOption, 0)
	if parsedURL.User != nil {
		authStr := parsedURL.User.String() + "@" + parsedURL.Host
		dialOptions = append(dialOptions, grpc.WithAuthority(authStr))
	}
	dialOptions = append(dialOptions, grpc.WithInsecure())

	// TODO(siggisim): Support GRPCS caches.
	grpcPort := getIntFlag("grpc_port", "1985")
	grpcsPort := getIntFlag("grpcs_port", "1986")
	if parsedURL.Port() == grpcsPort {
		parsedURL.Host = parsedURL.Hostname() + ":" + grpcPort
	}

	// Connect to host/port and create a new client
	conn, err := grpc.Dial(parsedURL.Host, dialOptions...)
	if err != nil {
		return err
	}
	defer conn.Close()
	client := bspb.NewByteStreamClient(conn)

	// Request the file bytestream
	req := &bspb.ReadRequest{
		ResourceName: strings.TrimPrefix(parsedURL.RequestURI(), "/"), // trim leading "/"
		ReadOffset:   0,                                               // started from the bottom now we here
		ReadLimit:    0,                                               // no limit
	}
	stream, err := client.Read(ctx, req)
	if err != nil {
		return err
	}

	for {
		rsp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		callback(rsp.Data)
	}
	return nil
}

func getIntFlag(flagName string, defaultVal string) string {
	f := flag.Lookup(flagName)
	if f == nil {
		return defaultVal
	}
	return f.Value.String()
}
