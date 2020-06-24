package bytestream

import (
	"context"
	"io"
	"net/url"
	"strings"
	"flag"

	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/environment"

	bspb "google.golang.org/genproto/googleapis/bytestream"
)

func StreamBytestreamFile(ctx context.Context, env environment.Env, url *url.URL, callback func([]byte)) error {
	if url.Scheme != "bytestream" {
		return status.InvalidArgumentErrorf("Only bytestream:// uris are supported")
	}

	err := error(nil)
	
	// If we have a cache enabled, try connecting to that first
	if env.GetCache() != nil {
		localURL, _ := url.Parse(url.String())
		localURL.Host = "localhost:"+getIntFlag("grpc_port", "1985")
		err = streamFromUrl(ctx, localURL, false, callback)
	}

	// If that fails, try to connect over grpcs
	if err != nil || env.GetCache() == nil {
		err = streamFromUrl(ctx, url, true, callback)
	}

	// If that fails, try grpc
	if err != nil {
		err = streamFromUrl(ctx, url, false, callback)
	}
	
	return err
}

func streamFromUrl(ctx context.Context, url *url.URL, grpcs bool, callback func([]byte)) (error) {
	if url.Port() == "" && grpcs {
		url.Host = url.Hostname() + ":443"
	} else if url.Port() == "" {
		url.Host = url.Hostname() + ":80"
	}

	conn, err := grpc_client.DialTargetWithOptions(url.String(), grpcs)

	if err != nil {
		return err
	}
	client := bspb.NewByteStreamClient(conn)

	// Request the file bytestream
	req := &bspb.ReadRequest{
		ResourceName: strings.TrimPrefix(url.RequestURI(), "/"), // trim leading "/"
		ReadOffset:   0,                                         // started from the bottom now we here
		ReadLimit:    0,                                         // no limit
	}
	readClient, err := client.Read(ctx, req)
	if err != nil {
		return err
	}

	defer conn.Close()
	for {
		rsp, err := readClient.Recv()
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