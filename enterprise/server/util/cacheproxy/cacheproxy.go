package cacheproxy

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gcodes "google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
)

const (
	downloadPath = "/download/"
	uploadPath   = "/upload/"

	hashParam      = "hash"
	sizeBytesParam = "size_bytes"
	prefixParam    = "prefix"
	offsetParam    = "offset"

	jwtHeader = "x-buildbuddy-jwt"
)

type CacheProxy struct {
	env        environment.Env
	cache      interfaces.Cache
	fileServer *http.Server
	client     *http.Client
}

func makeHTTP2Client() *http.Client {
	return &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
				return net.Dial(network, addr)
			},
		},
	}
}

func NewCacheProxy(env environment.Env, c interfaces.Cache, listenAddr string) *CacheProxy {
	mux := http.NewServeMux()
	proxy := &CacheProxy{
		env:    env,
		cache:  c,
		client: makeHTTP2Client(),
	}
	mux.Handle(downloadPath, proxy)
	mux.Handle(uploadPath, proxy)

	h2s := &http2.Server{}
	proxy.fileServer = &http.Server{
		Addr:    listenAddr,
		Handler: h2c.NewHandler(http.Handler(mux), h2s),
	}
	return proxy
}

func writeErr(err error, w http.ResponseWriter) {
	http.Error(w, err.Error(), http.StatusInternalServerError)
	return
}

func readDigest(r *http.Request) (*repb.Digest, error) {
	hash := r.URL.Query().Get(hashParam)
	sizeBytes := r.URL.Query().Get(sizeBytesParam)
	if hash == "" || sizeBytes == "" {
		return nil, status.InvalidArgumentErrorf("CacheProxy: hash (%q) and size_bytes (%q) params are required.", hash, sizeBytes)
	}
	n, err := strconv.ParseInt(sizeBytes, 10, 64)
	if err != nil {
		return nil, err
	}
	return &repb.Digest{
		Hash:      hash,
		SizeBytes: n,
	}, nil
}

func readJWT(ctx context.Context, r *http.Request) context.Context {
	if jwt := r.Header.Get(jwtHeader); jwt != "" {
		return context.WithValue(ctx, jwtHeader, jwt)
	}
	return ctx
}

func setJWT(ctx context.Context, r *http.Request) {
	if jwt, ok := ctx.Value(jwtHeader).(string); ok {
		r.Header.Set(jwtHeader, jwt)
	}
}

func contains(c context.Context, cache interfaces.Cache, d *repb.Digest, w http.ResponseWriter) {
	ok, err := cache.Contains(c, d)
	if err != nil {
		writeErr(err, w)
		return
	}
	if !ok {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	w.WriteHeader(200)
}

func reader(c context.Context, cache interfaces.Cache, d *repb.Digest, offset int64, w http.ResponseWriter) {
	r, err := cache.Reader(c, d, offset)
	if gstatus.Code(err) == gcodes.NotFound {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	if err != nil {
		writeErr(err, w)
		return
	}
	defer r.Close()
	_, err = io.Copy(w, r)
	if err != nil {
		writeErr(err, w)
		return
	}
}

func writer(c context.Context, cache interfaces.Cache, d *repb.Digest, r *http.Request, w http.ResponseWriter) {
	ok, err := cache.Contains(c, d)
	if err != nil && ok {
		w.WriteHeader(200)
		return
	}
	wc, err := cache.Writer(c, d)
	if err != nil {
		writeErr(err, w)
		return
	}
	_, err = io.Copy(wc, r.Body)
	if err != nil {
		writeErr(err, w)
		return
	}
	if err := wc.Close(); err != nil {
		writeErr(err, w)
		return
	}
	w.WriteHeader(200)
}

func (c *CacheProxy) Server() *http.Server {
	return c.fileServer
}

func (c *CacheProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx := readJWT(r.Context(), r)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, c.env)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	d, err := readDigest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	cache := c.cache.WithPrefix(r.URL.Query().Get(prefixParam))
	switch r.URL.Path {
	case downloadPath:
		{
			if r.Method == http.MethodHead {
				contains(ctx, cache, d, w)
				log.Debugf("CacheProxy(%s): /Contains %q took %s", c.fileServer.Addr, d.GetHash(), time.Since(start))
				return
			}
			if r.Method == http.MethodPost {
				offset := int64(0)
				if o := r.URL.Query().Get(offsetParam); o != "" {
					if n, err := strconv.ParseInt(o, 10, 64); err == nil {
						offset = n
					}
				}
				reader(ctx, cache, d, offset, w)
				log.Debugf("CacheProxy(%s): /Read %q took %s", c.fileServer.Addr, d.GetHash(), time.Since(start))
				return
			}
			writeErr(status.InvalidArgumentError("Invalid method (use HEAD or POST)"), w)
		}
	case uploadPath:
		{
			if r.Method == http.MethodPost {
				writer(ctx, cache, d, r, w)
				log.Debugf("CacheProxy(%s): /Write %q took %s", c.fileServer.Addr, d.GetHash(), time.Since(start))
				return
			}
			writeErr(status.InvalidArgumentError("Invalid method (use POST)"), w)
		}
	default:
		writeErr(status.InvalidArgumentErrorf("Invalid path %q", r.URL.Path), w)
	}
	_ = start
}

func (c *CacheProxy) remoteFileURL(peer, action, prefix, hash string, sizeBytes, offset int64) (string, error) {
	if !strings.HasPrefix(peer, "http") {
		peer = "http://" + peer
	}
	base, err := url.Parse(peer)
	if err != nil {
		return "", err
	}
	rel, err := base.Parse(action)
	if err != nil {
		return "", err
	}
	q := rel.Query()
	q.Set(prefixParam, prefix)
	q.Set(hashParam, hash)
	q.Set(sizeBytesParam, strconv.Itoa(int(sizeBytes)))
	q.Set(offsetParam, strconv.Itoa(int(offset)))
	rel.RawQuery = q.Encode()
	return rel.String(), nil
}

func (c *CacheProxy) RemoteContains(ctx context.Context, peer, prefix string, d *repb.Digest) (bool, error) {
	// Fast path: if peer is us, return local cache.
	if peer == c.fileServer.Addr {
		return c.cache.WithPrefix(prefix).Contains(ctx, d)
	}
	u, err := c.remoteFileURL(peer, downloadPath, prefix, d.GetHash(), d.GetSizeBytes(), 0)
	if err != nil {
		return false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, u, nil)
	if err != nil {
		return false, err
	}
	setJWT(ctx, req)
	rsp, err := c.client.Do(req)
	if err != nil {
		return false, status.UnavailableError(err.Error())
	}
	defer rsp.Body.Close()
	return rsp.StatusCode == 200, nil
}
func (c *CacheProxy) RemoteReader(ctx context.Context, peer, prefix string, d *repb.Digest, offset int64) (io.ReadCloser, error) {
	// Fast path: if peer is us, return local cache.
	if peer == c.fileServer.Addr {
		return c.cache.WithPrefix(prefix).Reader(ctx, d, offset)
	}
	u, err := c.remoteFileURL(peer, downloadPath, prefix, d.GetHash(), d.GetSizeBytes(), offset)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return nil, err
	}
	setJWT(ctx, req)
	rsp, err := c.client.Do(req)
	if err != nil {
		return nil, status.UnavailableError(err.Error())
	}
	if rsp.StatusCode == 200 {
		return rsp.Body, nil
	} else if rsp.StatusCode == 404 {
		return nil, status.NotFoundError("File not found (remotely).")
	} else {
		return nil, status.UnavailableError("Remote reader unavailable.")
	}
}

type PipeGroup struct {
	io.WriteCloser
	eg *errgroup.Group
}

func (p *PipeGroup) Close() error {
	if err := p.WriteCloser.Close(); err != nil {
		return err
	}
	return p.eg.Wait()
}

func (c *CacheProxy) RemoteWriter(ctx context.Context, peer, prefix string, d *repb.Digest) (io.WriteCloser, error) {
	// Fast path: if peer is us, return local cache.
	if peer == c.fileServer.Addr {
		return c.cache.WithPrefix(prefix).Writer(ctx, d)
	}
	u, err := c.remoteFileURL(peer, uploadPath, prefix, d.GetHash(), d.GetSizeBytes(), 0)
	if err != nil {
		return nil, err
	}
	reader, writer := io.Pipe()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, reader)
	if err != nil {
		return nil, status.UnavailableError(err.Error())
	}
	setJWT(ctx, req)
	eg, _ := errgroup.WithContext(ctx)
	eg.Go(func() error {
		rsp, err := c.client.Do(req)
		if err != nil {
			return err
		}
		return rsp.Body.Close()
	})
	return &PipeGroup{writer, eg}, nil
}
