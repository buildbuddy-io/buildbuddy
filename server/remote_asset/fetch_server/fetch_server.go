package fetch_server

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/ptypes"

	rapb "github.com/buildbuddy-io/buildbuddy/proto/remote_asset"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	durationpb "github.com/golang/protobuf/ptypes/duration"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	gcodes "google.golang.org/grpc/codes"
)

const (
	checksumQualifier = "checksum.sri"
	sha256Prefix      = "sha256-"
	maxHTTPTimeout    = 60 * time.Minute
)

type FetchServer struct {
	env   environment.Env
	cache interfaces.Cache
}

func NewFetchServer(env environment.Env) (*FetchServer, error) {
	cache := env.GetCache()
	if cache == nil {
		return nil, status.FailedPreconditionError("A cache is required to enable the ByteStreamServer")
	}
	return &FetchServer{
		env:   env,
		cache: cache,
	}, nil
}

func timeoutFromContext(ctx context.Context) (time.Duration, bool) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0, false
	}
	return deadline.Sub(time.Now()), true
}

func timeoutFromProto(timeout *durationpb.Duration) (time.Duration, bool) {
	if timeout != nil {
		if requestDuration, err := ptypes.Duration(timeout); err == nil {
			return requestDuration, true
		}
	}
	return 0, false
}

func timeoutHTTPClient(ctx context.Context, protoTimeout *durationpb.Duration) *http.Client {
	timeout := time.Duration(0)
	if ctxDuration, ok := timeoutFromContext(ctx); ok {
		timeout = ctxDuration
	}
	if protoDuration, ok := timeoutFromProto(protoTimeout); ok {
		timeout = protoDuration
	}
	if timeout == 0 || timeout > maxHTTPTimeout {
		timeout = maxHTTPTimeout
	}

	tp := &http.Transport{
		Dial: (&net.Dialer{
			Timeout: timeout,
		}).Dial,
		TLSHandshakeTimeout: timeout,
	}

	return &http.Client{
		Timeout:   timeout,
		Transport: tp,
	}
}

func (p *FetchServer) getCache(instanceName string) interfaces.Cache {
	c := p.cache
	if instanceName != "" {
		c = c.WithPrefix(instanceName)
	}
	return c
}

func (p *FetchServer) FetchBlob(ctx context.Context, req *rapb.FetchBlobRequest) (*rapb.FetchBlobResponse, error) {
	ctx, err := prefix.AttachUserPrefixToContext(ctx, p.env)
	if err != nil {
		return nil, err
	}
	cache := p.getCache(req.GetInstanceName())

	for _, qualifier := range req.GetQualifiers() {
		if qualifier.GetName() == checksumQualifier && strings.HasPrefix(qualifier.GetValue(), sha256Prefix) {
			b64sha256 := strings.TrimPrefix(qualifier.GetValue(), sha256Prefix)
			sha256, err := base64.StdEncoding.DecodeString(b64sha256)
			if err != nil {
				return nil, status.FailedPreconditionErrorf("Error decoding qualifier %q: %s", qualifier.GetName(), err.Error())
			}
			blobDigest := &repb.Digest{
				Hash:      fmt.Sprintf("%x", sha256),
				SizeBytes: int64(-1),
			}
			if data, err := cache.Get(ctx, blobDigest); err == nil {
				blobDigest.SizeBytes = int64(len(data)) // set the actual correct size.
				return &rapb.FetchBlobResponse{
					Status:     &statuspb.Status{Code: int32(gcodes.OK)},
					BlobDigest: blobDigest,
				}, nil
			}
		}
	}
	httpClient := timeoutHTTPClient(ctx, req.GetTimeout())
	for _, uri := range req.GetUris() {
		_, err := url.Parse(uri)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("Unparsable URI: %q", uri)
		}
		rsp, err := httpClient.Get(uri)
		if err != nil {
			return nil, status.AbortedErrorf("Error fetching URI  %q: %s", uri, err.Error())
		}
		data, err := ioutil.ReadAll(rsp.Body)
		if err != nil {
			log.Warningf("Error reading object bytes: %s", err.Error())
			continue
		}
		reader := bytes.NewReader(data)
		blobDigest, err := digest.Compute(reader)
		if err != nil {
			log.Warningf("Error computing object digest: %s", err.Error())
			continue
		}
		if err := cache.Set(ctx, blobDigest, data); err != nil {
			log.Warningf("Error inserting object into cache: %s", err.Error())
			continue
		}
		return &rapb.FetchBlobResponse{
			Uri:        uri,
			Status:     &statuspb.Status{Code: int32(gcodes.OK)},
			BlobDigest: blobDigest,
		}, nil
	}

	return &rapb.FetchBlobResponse{
		Status: &statuspb.Status{Code: int32(gcodes.NotFound)},
	}, nil
}

func (p *FetchServer) FetchDirectory(ctx context.Context, req *rapb.FetchDirectoryRequest) (*rapb.FetchDirectoryResponse, error) {
	return nil, status.UnimplementedError("FetchDirectory is not yet implemented")
}
