// Package credentialhelper fetches request headers from an external
// credential helper, so that commands can authenticate with credentials other
// than a BuildBuddy API key (for example, a short-lived token accepted by a
// proxy in front of BuildBuddy).
//
// Helpers speak Bazel's credential helper protocol:
// https://github.com/EngFlow/credential-helper-spec
package credentialhelper

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
)

// EnvVar names the credential helper executable. It is either a path or a
// name looked up on $PATH.
const EnvVar = "BUILDBUDDY_CREDENTIAL_HELPER"

type request struct {
	URI string `json:"uri"`
}

type response struct {
	Headers map[string][]string `json:"headers"`
	Expires time.Time           `json:"expires"`
}

var (
	mu    sync.Mutex
	cache = map[string]response{}
	now   = time.Now
)

// Enabled reports whether a credential helper is configured.
func Enabled() bool {
	return helperPath() != ""
}

func helperPath() string {
	return strings.TrimSpace(os.Getenv(EnvVar))
}

// AbsPath returns the configured helper as an absolute path, so that Bazel,
// which runs from the workspace root rather than bb's working directory, runs
// the same executable.
func AbsPath() (string, error) {
	helper := helperPath()
	resolved, err := exec.LookPath(helper)
	if err != nil {
		return "", fmt.Errorf("failed to find credential helper %q: %w", helper, err)
	}
	abs, err := filepath.Abs(resolved)
	if err != nil {
		return "", fmt.Errorf("failed to resolve credential helper %q: %w", helper, err)
	}
	return abs, nil
}

// Headers returns the headers the credential helper supplies for uri. The
// helper's response is reused until it expires, or for the life of the process
// if it carries no expiry. Returns nil when no helper is configured.
func Headers(ctx context.Context, uri string) (map[string][]string, error) {
	helper := helperPath()
	if helper == "" {
		return nil, nil
	}

	// Hold the lock while the helper runs so that concurrent RPCs start it
	// only once.
	mu.Lock()
	defer mu.Unlock()
	if rsp, ok := cache[uri]; ok && (rsp.Expires.IsZero() || now().Before(rsp.Expires)) {
		return rsp.Headers, nil
	}
	rsp, err := run(ctx, helper, uri)
	if err != nil {
		return nil, err
	}
	cache[uri] = rsp
	return rsp.Headers, nil
}

func run(ctx context.Context, helper, uri string) (response, error) {
	stdin, err := json.Marshal(request{URI: uri})
	if err != nil {
		return response{}, fmt.Errorf("failed to encode credential helper request: %w", err)
	}
	var stdout bytes.Buffer
	cmd := exec.CommandContext(ctx, helper, "get")
	cmd.Stdin = bytes.NewReader(stdin)
	cmd.Stdout = &stdout
	// The helper may need to prompt the user, e.g. to complete a login.
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return response{}, fmt.Errorf("failed to get credentials for %q from credential helper %q: %w", uri, helper, err)
	}
	var rsp response
	if err := json.Unmarshal(stdout.Bytes(), &rsp); err != nil {
		return response{}, fmt.Errorf("failed to parse response from credential helper %q: %w", helper, err)
	}
	return rsp, nil
}

// DialOptions returns dial options that attach the credential helper's headers
// to every RPC on a connection to target. It returns nil when no helper is
// configured, so it can be passed straight to grpc_client.DialSimple.
func DialOptions(target string) []grpc.DialOption {
	if !Enabled() {
		return nil
	}
	if !strings.Contains(target, "://") {
		target = "grpcs://" + target
	}
	return []grpc.DialOption{grpc.WithPerRPCCredentials(&rpcCredentials{uri: target})}
}

// SetHTTPHeaders adds the credential helper's headers for req's URL to req.
func SetHTTPHeaders(req *http.Request) error {
	headers, err := Headers(req.Context(), req.URL.String())
	if err != nil {
		return err
	}
	for name, values := range headers {
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}
	return nil
}

type rpcCredentials struct {
	uri string
}

func (c *rpcCredentials) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	headers, err := Headers(ctx, c.uri)
	if err != nil {
		return nil, err
	}
	md := make(map[string]string, len(headers))
	for name, values := range headers {
		md[strings.ToLower(name)] = strings.Join(values, ",")
	}
	return md, nil
}

// RequireTransportSecurity allows plaintext targets, matching the credentials
// grpc_client derives from a target's userinfo, so a local proxy can be used.
func (c *rpcCredentials) RequireTransportSecurity() bool {
	return false
}
