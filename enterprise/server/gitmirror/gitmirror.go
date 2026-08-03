// Package gitmirror implements a caching git mirror server. It serves the
// read-only half of the git smart HTTP protocol (upload-pack) from local
// bare mirrors of upstream repos, so that CI runners fetch from us instead
// of from the upstream git host.
//
// See README.md in this directory for more information.
package gitmirror

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httputil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/gitmirror/gitremote"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/gitmirror/gitstorage"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/go-git/go-git/v5/plumbing/format/pktline"
	"github.com/jonboulle/clockwork"
)

var (
	// TODO: allow configuring private IP ranges for git repos.
	// TODO: implement maintenance interval
	retentionPeriod     = flag.Duration("git.mirror.retention_period", 14*24*time.Hour, "How long to retain git repos that haven't been recently used. Set to 0 to disable retention.")
	maintenanceInterval = flag.Duration("git.mirror.maintenance_interval", 0, "How often to run maintenance on each mirrored repository (prune and repack). Set to 0 to disable periodic maintenance.")
	authCacheTTL        = flag.Duration("git.mirror.auth_cache_ttl", 5*time.Minute, "How long to reuse successful credential checks before validating them with the upstream git server.")
	redirectCacheTTL    = flag.Duration("git.mirror.redirect_cache_ttl", 10*time.Minute, "How long to cache resolved upstream repository URLs.")
	rootDir             = flag.String("git.mirror.root_directory", getDefaultRootDirectory(), "Directory in which bare repo mirrors are stored. Should be on a persistent volume.")
	insecureHTTPHosts   = flag.Slice("git.mirror.insecure_http_hosts", []string{}, "List of `host:port` values that should use plain HTTP transport.")

	// ErrInsufficientGitVersion indicates that Git is too old to run the mirror.
	ErrInsufficientGitVersion = errors.New("git 2.48 or newer is required")
	minimumGitVersion         = semver.MustParse("2.48.0")
)

const (
	currentVersion = "v1"
	infoRefsPath   = "/info/refs"
	uploadPackPath = "/git-upload-pack"
	// TODO: support write-through
	receivePackPath = "/git-receive-pack"
)

// Request is a parsed mirror request.
type Request struct {
	*http.Request

	// Version is our API's version string, currently "v1"
	Version string
	// RawRepository is the repo authority and path.
	RawRepository string
	// GitPath is a supported git endpoint.
	// If empty, the request is passed through.
	GitPath string

	// RawUpstream is the upstream authority and complete request path. It's
	// equivalent to the original request path but with the "/v1" prefix
	// removed.
	RawUpstream string
}

// ParseRequest validates and parses the versioned mirror path in req.
func ParseRequest(req *http.Request) (*Request, error) {
	var gitPath string
	switch {
	case strings.HasSuffix(req.URL.Path, infoRefsPath):
		gitPath = infoRefsPath
	case strings.HasSuffix(req.URL.Path, uploadPackPath):
		gitPath = uploadPackPath
	case strings.HasSuffix(req.URL.Path, receivePackPath):
		gitPath = receivePackPath
	}
	versionedPath, ok := strings.CutPrefix(req.URL.Path, "/")
	if !ok {
		return nil, errors.New("expected absolute request path")
	}
	version, rawUpstream, ok := strings.Cut(versionedPath, "/")
	if !ok || rawUpstream == "" {
		return nil, errors.New("missing upstream path")
	}
	if version != currentVersion {
		return nil, fmt.Errorf("unsupported route version %q", version)
	}
	escapedVersionedPath, ok := strings.CutPrefix(req.URL.EscapedPath(), "/")
	if !ok {
		return nil, errors.New("expected absolute escaped request path")
	}
	escapedVersion, escapedUpstream, ok := strings.Cut(escapedVersionedPath, "/")
	if !ok || escapedVersion != version {
		return nil, errors.New("invalid escaped upstream path")
	}
	var rawRepository string
	if gitPath != "" {
		rawRepository, ok = strings.CutSuffix(rawUpstream, gitPath)
		if !ok || rawRepository == "" {
			return nil, errors.New("missing repository")
		}
	}
	authority, _, _ := strings.Cut(rawUpstream, "/")
	if authority == "" || strings.ContainsAny(authority, `/\`) {
		return nil, fmt.Errorf("invalid upstream authority %q", authority)
	}
	// Accept repository paths even if they contain special characters.
	// In the gitstorage package, we sanitize these paths.
	return &Request{
		Request:       req,
		Version:       version,
		RawUpstream:   escapedUpstream,
		RawRepository: rawRepository,
		GitPath:       gitPath,
	}, nil
}

func getDefaultRootDirectory() string {
	if dir, err := os.UserCacheDir(); err == nil {
		return filepath.Join(dir, "buildbuddy", "git")
	}
	return filepath.Join(os.TempDir(), "buildbuddy", "git")
}

type forwardingTransport struct {
	client      *gitremote.Client
	rawUpstream string
}

func (t *forwardingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.client.Forward(req, t.rawUpstream)
}

type server struct {
	storage *gitstorage.Storage

	// Client instance. Only a single instance is used so that we can reuse
	// connections.
	client *gitremote.Client
}

func checkGitVersion() error {
	out, err := exec.Command("git", "version").Output()
	if err != nil {
		return fmt.Errorf("query git version: %w", err)
	}
	fields := strings.Fields(string(out))
	if len(fields) < 3 || fields[0] != "git" || fields[1] != "version" {
		return fmt.Errorf("parse git version output %q", strings.TrimSpace(string(out)))
	}
	version, err := semver.NewVersion(fields[2])
	if err != nil {
		return fmt.Errorf("parse git version %q: %w", fields[2], err)
	}
	if version.LessThan(minimumGitVersion) {
		return fmt.Errorf("%w; found %s", ErrInsufficientGitVersion, version)
	}
	return nil
}

// New creates a Git mirror server backed by the configured storage directory.
func New(clock clockwork.Clock) (*server, error) {
	if err := checkGitVersion(); err != nil {
		return nil, err
	}
	client, err := gitremote.NewClient(gitremote.ClientOptions{
		Clock:                 clock,
		AuthorizationCacheTTL: *authCacheTTL,
		RedirectCacheTTL:      *redirectCacheTTL,
		InsecureHTTPHosts:     *insecureHTTPHosts,
	})
	if err != nil {
		return nil, err
	}
	storage, err := gitstorage.New(*rootDir, clock, *retentionPeriod)
	if err != nil {
		return nil, err
	}
	s := &server{
		storage: storage,
		client:  client,
	}
	return s, nil
}

var _ http.Handler = (*server)(nil)

// RootDir returns the server's repository storage directory.
func (s *server) RootDir() string {
	return s.storage.RootDir()
}

// Close stops the server's background work.
func (s *server) Close() error {
	return s.storage.Close()
}

func (s *server) forward(w http.ResponseWriter, req *Request) {
	proxy := &httputil.ReverseProxy{
		Rewrite: func(*httputil.ProxyRequest) {},
		Transport: &forwardingTransport{
			client:      s.client,
			rawUpstream: req.RawUpstream,
		},
		ErrorHandler: func(w http.ResponseWriter, httpReq *http.Request, err error) {
			log.CtxErrorf(httpReq.Context(), "Failed to forward Git request: %s", err)
			http.Error(w, "failed to forward request", http.StatusBadGateway)
		},
	}
	proxy.ServeHTTP(w, req.Request)
}

// ServeHTTP parses and dispatches Git smart HTTP requests.
func (s *server) ServeHTTP(w http.ResponseWriter, httpReq *http.Request) {
	req, err := ParseRequest(httpReq)
	if err != nil {
		http.Error(w, "invalid upstream path", http.StatusBadRequest)
		return
	}
	switch req.GitPath {
	case infoRefsPath:
		service := req.URL.Query().Get("service")
		if req.Method == http.MethodGet && service == "git-upload-pack" {
			s.handleRefs(w, req)
			return
		}
	case uploadPackPath:
		if req.Method == http.MethodPost {
			s.handleUploadPack(w, req)
			return
		}
	}
	s.forward(w, req)
}

// handleRefs serves the discovery phase of Git's smart HTTP protocol.
//
// See PRIMER.md for more details.
func (s *server) handleRefs(w http.ResponseWriter, req *Request) {
	ctx := req.Context()

	// Validate credentials and resolve redirects before passing the URL to git.
	// When later calling git, we disable redirects, since only the resolved
	// endpoint was validated.
	authorization := req.Header.Get("Authorization")
	upstreamRepo, err := s.client.Resolve(ctx, req.RawRepository, authorization)
	if err != nil {
		statusCode := http.StatusBadGateway
		if httpErr, ok := errors.AsType[*gitremote.HTTPError](err); ok {
			statusCode = httpErr.StatusCode
			maps.Copy(w.Header(), httpErr.Header)
		}
		http.Error(w, err.Error(), statusCode)
		return
	}
	storedRepo, release := s.storage.Acquire(upstreamRepo)
	defer release()
	protocolV2 := false
	for parameter := range strings.SplitSeq(req.Header.Get("Git-Protocol"), ":") {
		if parameter == "version=2" {
			protocolV2 = true
			break
		}
	}
	if protocolV2 {
		// In protocol v2, the client isn't asking for refs yet, so we don't
		// need to fetch the upstream repo. Just init the repo so that we can
		// run git commands from it, in order to advertise our capabilities.
		err = storedRepo.Initialize(ctx)
	} else {
		// Older protocols (v0 and v1) advertise refs in this response, so we
		// need to refresh them first to avoid returning stale refs. For now,
		// just do a full fetch from the remote.
		var gitFlags []string
		gitFlags, err = upstreamRepo.GitFlags()
		if err == nil {
			err = storedRepo.Fetch(ctx, authorization, gitFlags...)
		}
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	// Generate the refs and capabilities advertised to the client. Forward the
	// Git-Protocol header value as an env var so that upload-pack generates the
	// response appropriate to the protocol version.
	cmd := exec.CommandContext(ctx, "git", "upload-pack", "--advertise-refs", storedRepo.Path())
	if protocol := req.Header.Get("Git-Protocol"); protocol != "" {
		cmd.Env = append(os.Environ(), "GIT_PROTOCOL="+protocol)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	advertisement, err := cmd.Output()
	if err != nil {
		log.CtxErrorf(ctx, "Git upload-pack advertisement failed: %s: %q", err, stderr.String())
		http.Error(w, "failed to advertise repository", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
	w.Header().Set("Cache-Control", "no-cache")
	// Smart HTTP protocol requires the Git advertisement to be prefixed with
	// a service announcement.
	encoder := pktline.NewEncoder(w)
	if err := encoder.EncodeString("# service=git-upload-pack\n"); err != nil && ctx.Err() != nil {
		log.CtxWarningf(ctx, "Failed to write service announcement: %s", err)
		return
	}
	if err := encoder.Flush(); err != nil && ctx.Err() != nil {
		log.CtxWarningf(ctx, "Failed to flush service announcement: %s", err)
		return
	}
	_, err = w.Write(advertisement)
	if err != nil && ctx.Err() != nil {
		log.CtxWarningf(ctx, "Failed to write git advertisement: %s", err)
		return
	}
}

// handleUploadPack negotiates and transmits a packfile to the client.
//
// The mirror runs git upload-pack --stateless-rpc for each fetch request. Each
// upload-pack process handles a single request and then exits. No process state
// is shared between HTTP requests, so a POST must not depend on its discovery
// request reaching the same mirror backend.
//
// See PRIMER.md for more details.
func (s *server) handleUploadPack(w http.ResponseWriter, req *Request) {
	ctx := req.Context()

	// Git gzip-compresses larger negotiation requests.
	// Go's HTTP server leaves request decompression to the handler.
	body := io.Reader(req.Body)
	switch strings.ToLower(strings.TrimSpace(req.Header.Get("Content-Encoding"))) {
	case "":
	case "gzip":
		compressedBody, err := gzip.NewReader(req.Body)
		if err != nil {
			http.Error(w, "invalid gzip request body", http.StatusBadRequest)
			return
		}
		defer compressedBody.Close()
		body = compressedBody
	default:
		http.Error(w, "unsupported content encoding", http.StatusUnsupportedMediaType)
		return
	}

	// Validate credentials and resolve redirects before passing the URL to git.
	// Note: When later calling git, we disable redirects, since only the
	// resolved endpoint was validated.
	authorization := req.Header.Get("Authorization")
	upstreamRepo, err := s.client.Resolve(ctx, req.RawRepository, authorization)
	if err != nil {
		statusCode := http.StatusBadGateway
		if httpErr, ok := errors.AsType[*gitremote.HTTPError](err); ok {
			statusCode = httpErr.StatusCode
			maps.Copy(w.Header(), httpErr.Header)
		}
		http.Error(w, err.Error(), statusCode)
		return
	}
	storedRepo, release := s.storage.Acquire(upstreamRepo)
	defer release()

	// The discovery and upload-pack requests may reach different backends, so
	// refresh this backend before serving objects from its local repository.
	// TODO: Parse the protocol command and fetch only requested objects missing
	// locally. Continue refreshing all refs before responding to ls-refs.
	gitFlags, err := upstreamRepo.GitFlags()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	if err := storedRepo.Fetch(ctx, authorization, gitFlags...); err != nil {
		log.CtxErrorf(ctx, "Failed to refresh repository: %s", err)
		http.Error(w, "failed to refresh repository", http.StatusBadGateway)
		return
	}

	// --stateless-rpc tells git to process a single Git protocol request from
	// stdin, write its response to stdout, and exit.
	cmd := exec.CommandContext(ctx, "git", "upload-pack", "--stateless-rpc", storedRepo.Path())
	cmd.Stdin = body
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.CtxErrorf(ctx, "Failed to open git upload-pack output: %s", err)
		http.Error(w, "failed to serve repository", http.StatusInternalServerError)
		return
	}

	// Protocol v2 is selected through this environment variable rather than
	// through the packet stream, so preserve the client's Git-Protocol header.
	if protocol := req.Header.Get("Git-Protocol"); protocol != "" {
		cmd.Env = append(os.Environ(), "GIT_PROTOCOL="+protocol)
	}
	if err := cmd.Start(); err != nil {
		log.CtxErrorf(ctx, "Failed to start git upload-pack: %s", err)
		http.Error(w, "failed to serve repository", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
	w.Header().Set("Cache-Control", "no-cache")
	bytesWritten, streamErr := io.Copy(w, stdout)
	if streamErr != nil {
		// Stop upload-pack if the client disconnects or the response cannot be
		// written; otherwise it could remain blocked writing to the pipe.
		_ = stdout.Close()
	}
	waitErr := cmd.Wait()
	if streamErr != nil {
		log.CtxWarningf(ctx, "Failed to stream git upload-pack response: %s", streamErr)
		return
	}
	if waitErr != nil {
		log.CtxErrorf(ctx, "Git upload-pack failed: %s", waitErr)
		if bytesWritten == 0 {
			w.Header().Del("Content-Type")
			http.Error(w, "failed to serve repository", http.StatusInternalServerError)
		}
	}
}
