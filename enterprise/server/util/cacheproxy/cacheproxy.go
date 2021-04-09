package cacheproxy

import (
	"context"
	"crypto/tls"
	"fmt"
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
	batchContainsPath = "/batch/contains/"
	batchDownloadPath = "/batch/download/"
	downloadPath      = "/download/"
	uploadPath        = "/upload/"

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
	proxy := &CacheProxy{
		env:    env,
		cache:  c,
		client: makeHTTP2Client(),
	}
	h2s := &http2.Server{}
	proxy.fileServer = &http.Server{
		Addr:    listenAddr,
		Handler: h2c.NewHandler(proxy, h2s),
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

func parseValues(r io.Reader) (*url.Values, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	values, err := url.ParseQuery(string(body))
	if err != nil {
		return nil, err
	}
	return &values, nil
}

func containsMulti(c context.Context, cache interfaces.Cache, r *http.Request, w http.ResponseWriter) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	values, err := parseValues(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	digests := make([]*repb.Digest, 0)
	for hash, values := range *values {
		if len(values) != 1 {
			http.Error(w, "wrong number of values", http.StatusInternalServerError)
			return
		}
		n, err := strconv.ParseInt(values[0], 10, 64)
		if err != nil {
			http.Error(w, "error parsing sizeBytes", http.StatusInternalServerError)
			return
		}
		digests = append(digests, &repb.Digest{
			Hash:      hash,
			SizeBytes: n,
		})
	}
	found, err := cache.ContainsMulti(c, digests)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data := url.Values{}
	for d, exists := range found {
		if exists {
			data.Set(d.GetHash(), "1")
		}
	}
	w.Write([]byte(data.Encode()))
	w.WriteHeader(200)
}

func getMulti(c context.Context, cache interfaces.Cache, r *http.Request, w http.ResponseWriter) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	values, err := parseValues(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	digests := make([]*repb.Digest, 0)
	for hash, values := range *values {
		if len(values) != 1 {
			http.Error(w, "wrong number of values", http.StatusInternalServerError)
			return
		}
		n, err := strconv.ParseInt(values[0], 10, 64)
		if err != nil {
			http.Error(w, "error parsing sizeBytes", http.StatusInternalServerError)
			return
		}
		digests = append(digests, &repb.Digest{
			Hash:      hash,
			SizeBytes: n,
		})
	}
	got, err := cache.GetMulti(c, digests)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data := url.Values{}
	for d, buf := range got {
		data.Set(d.GetHash(), string(buf))
	}
	w.Write([]byte(data.Encode()))
	w.WriteHeader(200)
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
	cache := c.cache.WithPrefix(r.URL.Query().Get(prefixParam))

	switch r.URL.Path {
	case batchContainsPath:
		{
			if r.Method == http.MethodPost {
				containsMulti(ctx, cache, r, w)
				log.Debugf("CacheProxy(%s): /ContainsMulti took %s", c.fileServer.Addr, time.Since(start))
				return
			}
			writeErr(status.InvalidArgumentError("Invalid method (use POST)"), w)
		}
	case batchDownloadPath:
		{
			if r.Method == http.MethodPost {
				getMulti(ctx, cache, r, w)
				log.Debugf("CacheProxy(%s): /GetMulti took %s", c.fileServer.Addr, time.Since(start))
				return
			}
			writeErr(status.InvalidArgumentError("Invalid method (use POST)"), w)
		}
	case downloadPath:
		{
			d, err := readDigest(r)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

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
			d, err := readDigest(r)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

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

func (c *CacheProxy) remoteBatchURL(peer, action, prefix string) (string, error) {
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
	rel.RawQuery = q.Encode()
	return rel.String(), nil
}

func (c *CacheProxy) RemoteContainsMulti(ctx context.Context, peer, prefix string, digests []*repb.Digest) (map[*repb.Digest]bool, error) {
	u, err := c.remoteBatchURL(peer, batchContainsPath, prefix)
	if err != nil {
		return nil, err
	}
	data := url.Values{}
	for _, d := range digests {
		data.Set(d.GetHash(), fmt.Sprintf("%d", d.GetSizeBytes()))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}
	setJWT(ctx, req)
	rsp, err := c.client.Do(req)
	if err != nil {
		return nil, status.UnavailableError(err.Error())
	}
	defer rsp.Body.Close()
	values, err := parseValues(rsp.Body)
	if err != nil {
		return nil, err
	}
	results := make(map[*repb.Digest]bool, 0)
	for _, d := range digests {
		found := values.Get(d.GetHash()) == "1"
		results[d] = found
	}
	return results, nil
}

func (c *CacheProxy) RemoteGetMulti(ctx context.Context, peer, prefix string, digests []*repb.Digest) (map[*repb.Digest][]byte, error) {
	u, err := c.remoteBatchURL(peer, batchDownloadPath, prefix)
	if err != nil {
		return nil, err
	}
	data := url.Values{}
	for _, d := range digests {
		data.Set(d.GetHash(), fmt.Sprintf("%d", d.GetSizeBytes()))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, err
	}
	setJWT(ctx, req)
	rsp, err := c.client.Do(req)
	if err != nil {
		return nil, status.UnavailableError(err.Error())
	}
	defer rsp.Body.Close()
	values, err := parseValues(rsp.Body)
	if err != nil {
		return nil, err
	}
	results := make(map[*repb.Digest][]byte, 0)
	for _, d := range digests {
		if buf := values.Get(d.GetHash()); len(buf) > 0 {
			results[d] = []byte(buf)
		}
	}
	return results, nil
}

// AutoCloser closes the provided ReadCloser upon the
// first call to Read which returns a non-nil error.
type AutoCloser struct {
	io.ReadCloser
}

func (c *AutoCloser) Read(data []byte) (int, error) {
	n, err := c.ReadCloser.Read(data)
	if err != nil {
		defer c.ReadCloser.Close()
	}
	return n, err
}

func (c *CacheProxy) RemoteReader(ctx context.Context, peer, prefix string, d *repb.Digest, offset int64) (io.ReadCloser, error) {
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
