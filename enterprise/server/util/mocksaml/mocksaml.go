// Package mocksaml provides a way to run a mock SAML IDP for testing.
//
// Currently, it uses https://github.com/boxyhq/mock-saml, and requires
// docker to be available.
package mocksaml

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	// SignInButtonSelector is the CSS selector targeting the "Sign In" button
	// in the login page.
	SignInButtonSelector = "button.btn-primary"

	// DefaultUserEmail is the username that is pre-filled when visiting the
	// mock SAML sign in page.
	DefaultUserEmail = "jackson@example.com"

	// Mirror of boxyhq/mock-saml
	image = "gcr.io/flame-public/mock-saml@sha256:3d297c10f0490059a4efe00922c42099f4553a3ccd8cab8b90096057dc8d93d0"

	entityID = "https://saml.example.com/entityid"
)

// IDP represents a running mock SAML identity provider (IDP).
type IDP struct {
	containerID string
	cert        string
	port        int
}

// Start starts a mock SAML IDP server.
func Start(port int, cert, key io.Reader) (*IDP, error) {
	certBytes, err := io.ReadAll(cert)
	if err != nil {
		return nil, fmt.Errorf("read cert: %w", err)
	}
	keyBytes, err := io.ReadAll(key)
	if err != nil {
		return nil, fmt.Errorf("read key: %w", err)
	}
	cmd := exec.Command(
		"docker", "run", "--rm", "--detach",
		fmt.Sprintf("--publish=%d:4000", port),
		fmt.Sprintf("--env=APP_URL=http://localhost:%d", port),
		fmt.Sprintf("--env=PUBLIC_KEY=%s", base64.StdEncoding.EncodeToString(certBytes)),
		fmt.Sprintf("--env=PRIVATE_KEY=%s", base64.StdEncoding.EncodeToString(keyBytes)),
		fmt.Sprintf("--env=ENTITY_ID=%s", entityID),
		image,
	)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("start mocksaml container: %w: %q", err, strings.TrimSpace(stderr.String()))
	}
	cid := strings.TrimSpace(stdout.String())
	return &IDP{
		containerID: cid,
		port:        port,
	}, nil
}

// WaitUntilReady waits for the server to become ready to serve requests.
func (s *IDP) WaitUntilReady(ctx context.Context) error {
	for {
		req, err := http.NewRequestWithContext(ctx, "GET", s.MetadataURL(), nil)
		if err != nil {
			return fmt.Errorf("create request: %w", err)
		}
		rsp, err := http.DefaultClient.Do(req)
		if err == nil && rsp.StatusCode == 200 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
			continue
		}
	}
}

func (s *IDP) Kill() error {
	b, err := exec.Command("docker", "kill", "--signal=KILL", s.containerID).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker kill: %w: %s", err, strings.TrimSpace(string(b)))
	}
	return nil
}

// Wait waits until the server has exited.
func (s *IDP) Wait(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "docker", "wait", s.containerID)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker wait: %w: %q", err, strings.TrimSpace(stderr.String()))
	}
	rawCode := strings.TrimSpace(stdout.String())
	exitCode, err := strconv.Atoi(rawCode)
	if err != nil {
		return fmt.Errorf("docker wait: failed to parse exit code from %q: %w", rawCode, err)
	}
	if exitCode == 0 {
		return nil
	}
	return fmt.Errorf("exit code %d", exitCode)
}

func (s *IDP) BuildBuddyLoginURL(buildbuddyURL, slug string) string {
	query := url.Values{
		"acsUrl":   []string{buildbuddyURL + "/auth/saml/acs?slug=" + slug},
		"audience": []string{buildbuddyURL + "/saml/metadata?slug=" + slug},
		// Need to set relayState otherwise it winds up being "undefined"
		// and we get redirected to /auth/saml/undefined URL after login.
		"relayState": []string{""},
	}.Encode()
	return fmt.Sprintf("http://localhost:%d/saml/login?%s", s.port, query)
}

func (s *IDP) MetadataURL() string {
	return fmt.Sprintf("http://localhost:%d/api/saml/metadata", s.port)
}

func (s *IDP) CopyLogs(stdout, stderr io.Writer) error {
	cmd := exec.Command("docker", "logs", "--follow", s.containerID)
	cmd.Stderr = stdout
	cmd.Stdout = stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker logs: %w", err)
	}
	return nil
}
