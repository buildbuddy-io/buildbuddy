package bytestream

import (
	"context"
	"io"
	"net/url"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"

	bspb "google.golang.org/genproto/googleapis/bytestream"
)

func StreamBytestreamFile(ctx context.Context, url *url.URL, callback func([]byte)) error {
	if url.Scheme != "bytestream" {
		return status.InvalidArgumentErrorf("Only bytestream:// uris are supported")
	}

	// Try to connect over grpcs
	stream, conn, err := readStreamFromUrl(ctx, url, true)

	// If that fails, try grpc
	if err != nil || stream == nil {
		conn.Close()
		// TODO(siggisim): Remove this fallback by examining flags on invocation.
		stream, conn, err = readStreamFromUrl(ctx, url, false)
	}
	if err != nil || stream == nil || conn == nil {
		return err
	}
	defer conn.Close()

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

func readStreamFromUrl(ctx context.Context, url *url.URL, grpcs bool) (bspb.ByteStream_ReadClient, *grpc.ClientConn, error) {
	if url.Port() == "" && grpcs {
		url.Host = url.Hostname() + ":443"
	} else if url.Port() == "" {
		url.Host = url.Hostname() + ":80"
	}

	conn, err := grpc_client.DialTargetWithOptions(url.String(), grpcs)

	if err != nil {
		return nil, nil, err
	}
	client := bspb.NewByteStreamClient(conn)

	// Request the file bytestream
	req := &bspb.ReadRequest{
		ResourceName: strings.TrimPrefix(url.RequestURI(), "/"), // trim leading "/"
		ReadOffset:   0,                                         // started from the bottom now we here
		ReadLimit:    0,                                         // no limit
	}
	readClient, err := client.Read(ctx, req)
	return readClient, conn, err
}
