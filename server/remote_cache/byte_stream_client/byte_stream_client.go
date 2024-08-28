package byte_stream_client

import (
	"context"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/cache_api_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/urlutil"
	"google.golang.org/grpc"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	restrictBytestreamDialing = flag.Bool("app.restrict_bytestream_dialing", false, "If true, only allow dialing localhost or the configured cache backend for bytestream requests.")
	enablePoolCache           = flag.Bool("grpc_client.enable_pool_cache", false, "Whether or not to enable the connection pool cache.")
)

type pooledByteStreamClient struct {
	env         environment.Env
	connMutex   sync.Mutex
	connPoolMap map[string]grpc.ClientConnInterface
}

func RegisterPooledBytestreamClient(env *real_environment.RealEnv) {
	p := NewPooledByteStreamClient(env)
	env.SetPooledByteStreamClient(p)
}

func NewPooledByteStreamClient(env environment.Env) *pooledByteStreamClient {
	return &pooledByteStreamClient{
		env:         env,
		connPoolMap: make(map[string]grpc.ClientConnInterface),
	}
}

func (p *pooledByteStreamClient) StreamBytestreamFile(ctx context.Context, url *url.URL, writer io.Writer) error {
	return p.StreamBytestreamFileChunk(ctx, url, 0, 0, writer)
}

func (p *pooledByteStreamClient) StreamBytestreamFileChunk(ctx context.Context, url *url.URL, offset int64, limit int64, writer io.Writer) error {
	if url.Scheme != "bytestream" && url.Scheme != "actioncache" {
		return status.InvalidArgumentErrorf("Only bytestream:// uris are supported")
	}

	var err error

	// If we have a cache enabled, try connecting to that first
	if p.env.GetCache() != nil {
		localURL, _ := url.Parse(url.String())
		grpcPort := "1985"
		if p, err := flagutil.GetDereferencedValue[int]("grpc_port"); err == nil {
			grpcPort = strconv.Itoa(p)
		}
		localURL.Host = "localhost:" + grpcPort
		err = p.streamFromUrl(ctx, localURL, false, offset, limit, writer)
	}

	// If the local cache did not work, maybe a remote cache is being used.
	// Try to connect to that, first over grpcs.
	if err != nil || p.env.GetCache() == nil {
		err = p.streamFromUrl(ctx, url, true, offset, limit, writer)
	}

	// If that didn't work, try plain old grpc.
	if err != nil {
		err = p.streamFromUrl(ctx, url, false, offset, limit, writer)
	}

	// Sanitize the error so as to not expose internal services via the
	// error message.
	if err != nil {
		if !status.IsNotFoundError(err) {
			log.Warningf("Error byte-streaming from %q: %s", stripUser(url), err)
		}
		return status.UnavailableErrorf("failed to read byte stream resource %q", stripUser(url))
	}
	return nil
}

func stripUser(u *url.URL) *url.URL {
	copy := *u // shallow copy
	copy.User = nil
	return &copy
}

func getTargetForURL(u *url.URL, grpcs bool) string {
	target := url.URL{Scheme: "grpc", User: u.User, Host: u.Host}
	if grpcs {
		target.Scheme = "grpcs"
		if u.Port() == "" {
			target.Host = u.Hostname() + ":443"
		}
	} else if u.Port() == "" {
		target.Host = u.Hostname() + ":80"
	}

	return target.String()
}

func isPermittedForDial(target string) bool {
	u, err := url.Parse(target)
	if err != nil {
		return false
	}
	if cache_api_url.String() == "" {
		return true
	}

	return u.Hostname() == "localhost" || (urlutil.GetDomain(u.Hostname()) == urlutil.GetDomain(cache_api_url.WithPath("").Hostname()))
}

func (p *pooledByteStreamClient) streamFromUrl(ctx context.Context, url *url.URL, grpcs bool, offset int64, limit int64, writer io.Writer) (err error) {
	target := getTargetForURL(url, grpcs)
	if *restrictBytestreamDialing && !isPermittedForDial(target) {
		return status.InvalidArgumentErrorf("Tried to connect to an unpermitted domain: %s", target)
	}

	var conn grpc.ClientConnInterface
	if *enablePoolCache {
		conn, err = p.getGrpcClientConnPoolForURL(target)
		if err != nil {
			return err
		}
	} else {
		closeableConn, err := grpc_client.DialInternalWithoutPooling(p.env, target)
		if err != nil {
			return err
		}
		defer closeableConn.Close()
		conn = closeableConn
	}

	if url.Scheme == "actioncache" {
		acClient := repb.NewActionCacheClient(conn)
		r, err := digest.ParseActionCacheResourceName(strings.TrimPrefix(url.RequestURI(), "/"))
		if err != nil {
			return err
		}

		// Request the ActionResult
		req := &repb.GetActionResultRequest{
			InstanceName:   r.GetInstanceName(),
			ActionDigest:   r.GetDigest(),
			DigestFunction: r.GetDigestFunction(),
		}
		actionResult, err := acClient.GetActionResult(ctx, req)
		if err != nil {
			return err
		}

		// Turn ActionResult into []byte
		buf, err := proto.Marshal(actionResult)
		if err != nil {
			return err
		}
		if _, err := writer.Write(buf); err != nil {
			return err
		}
		return nil
	}
	client := bspb.NewByteStreamClient(conn)

	// Request the file bytestream
	req := &bspb.ReadRequest{
		ResourceName: strings.TrimPrefix(url.RequestURI(), "/"), // trim leading "/"
		ReadOffset:   offset,                                    // started from the bottom now we here
		ReadLimit:    limit,                                     // no limit
	}
	readClient, err := client.Read(ctx, req)
	if err != nil {
		return err
	}

	for {
		rsp, err := readClient.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if _, err := writer.Write(rsp.Data); err != nil {
			return err
		}
	}
	return nil
}

func (p *pooledByteStreamClient) getGrpcClientConnPoolForURL(target string) (conn grpc.ClientConnInterface, err error) {
	p.connMutex.Lock()
	defer p.connMutex.Unlock()
	connPool, ok := p.connPoolMap[target]
	if ok && connPool != nil {
		return connPool, nil
	}

	// We didn't find a connection pool, so we'll make one.
	connPool, err = grpc_client.DialInternal(p.env, target)
	if err != nil {
		return nil, err
	}
	p.connPoolMap[target] = connPool
	log.Infof("Cached connection pool for target: %s, %d targets cached so far", target, len(p.connPoolMap))
	return connPool, nil
}
