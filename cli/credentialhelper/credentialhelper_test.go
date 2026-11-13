package credentialhelper

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// writeHelper writes a credential helper that records each request it gets in
// a log file and replies with stdout.
func writeHelper(t *testing.T, stdout string, exitCode int) (helper, requestLog string) {
	t.Helper()
	dir := t.TempDir()
	requestLog = filepath.Join(dir, "requests")
	helper = filepath.Join(dir, "helper")
	script := "#!/bin/sh\n" +
		"[ \"$1\" = get ] || exit 2\n" +
		"cat >> '" + requestLog + "'\n" +
		"echo >> '" + requestLog + "'\n" +
		"printf '%s' '" + stdout + "'\n" +
		"exit " + strconv.Itoa(exitCode) + "\n"
	require.NoError(t, os.WriteFile(helper, []byte(script), 0o755))
	return helper, requestLog
}

func requests(t *testing.T, requestLog string) []string {
	t.Helper()
	b, err := os.ReadFile(requestLog)
	if os.IsNotExist(err) {
		return nil
	}
	require.NoError(t, err)
	return strings.Split(strings.TrimSpace(string(b)), "\n")
}

func reset(t *testing.T) {
	t.Helper()
	mu.Lock()
	cache = map[string]response{}
	mu.Unlock()
	t.Cleanup(func() { now = time.Now })
}

func TestHeaders(t *testing.T) {
	ctx := context.Background()

	t.Run("no helper configured", func(t *testing.T) {
		reset(t)
		t.Setenv(EnvVar, "")
		headers, err := Headers(ctx, "grpcs://example.com")
		require.NoError(t, err)
		require.Nil(t, headers)
		require.False(t, Enabled())
		require.Nil(t, DialOptions("example.com"))
	})

	t.Run("returns helper headers and reuses them", func(t *testing.T) {
		reset(t)
		helper, requestLog := writeHelper(t, `{"headers":{"Authorization":["Bearer abc"]}}`, 0)
		t.Setenv(EnvVar, helper)

		for range 2 {
			headers, err := Headers(ctx, "grpcs://example.com")
			require.NoError(t, err)
			require.Equal(t, map[string][]string{"Authorization": {"Bearer abc"}}, headers)
		}
		require.Equal(t, []string{`{"uri":"grpcs://example.com"}`}, requests(t, requestLog))
	})

	t.Run("runs the helper again once the response expires", func(t *testing.T) {
		reset(t)
		expires := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
		helper, requestLog := writeHelper(t, `{"headers":{"Authorization":["Bearer abc"]},"expires":"2030-01-01T00:00:00Z"}`, 0)
		t.Setenv(EnvVar, helper)

		now = func() time.Time { return expires.Add(-time.Minute) }
		_, err := Headers(ctx, "grpcs://example.com")
		require.NoError(t, err)
		_, err = Headers(ctx, "grpcs://example.com")
		require.NoError(t, err)
		require.Len(t, requests(t, requestLog), 1)

		now = func() time.Time { return expires }
		_, err = Headers(ctx, "grpcs://example.com")
		require.NoError(t, err)
		require.Len(t, requests(t, requestLog), 2)
	})

	t.Run("caches per URI", func(t *testing.T) {
		reset(t)
		helper, requestLog := writeHelper(t, `{"headers":{}}`, 0)
		t.Setenv(EnvVar, helper)

		_, err := Headers(ctx, "grpcs://a.example.com")
		require.NoError(t, err)
		_, err = Headers(ctx, "https://b.example.com/file/download")
		require.NoError(t, err)
		require.Equal(t, []string{
			`{"uri":"grpcs://a.example.com"}`,
			`{"uri":"https://b.example.com/file/download"}`,
		}, requests(t, requestLog))
	})

	t.Run("helper fails", func(t *testing.T) {
		reset(t)
		helper, _ := writeHelper(t, "", 1)
		t.Setenv(EnvVar, helper)

		_, err := Headers(ctx, "grpcs://example.com")
		require.ErrorContains(t, err, "failed to get credentials")
	})

	t.Run("helper returns invalid JSON", func(t *testing.T) {
		reset(t)
		helper, _ := writeHelper(t, "not json", 0)
		t.Setenv(EnvVar, helper)

		_, err := Headers(ctx, "grpcs://example.com")
		require.ErrorContains(t, err, "failed to parse response")
	})
}

func TestSetHTTPHeaders(t *testing.T) {
	reset(t)
	helper, requestLog := writeHelper(t, `{"headers":{"Authorization":["Bearer abc"]}}`, 0)
	t.Setenv(EnvVar, helper)

	req, err := http.NewRequest(http.MethodGet, "https://app.example.com/file/download?invocation_id=1", nil)
	require.NoError(t, err)
	req.Header.Set("x-buildbuddy-api-key", "key")
	require.NoError(t, SetHTTPHeaders(req))

	require.Equal(t, "Bearer abc", req.Header.Get("Authorization"))
	require.Equal(t, "key", req.Header.Get("x-buildbuddy-api-key"))
	require.Equal(t, []string{`{"uri":"https://app.example.com/file/download?invocation_id=1"}`}, requests(t, requestLog))
}

func TestRPCCredentials(t *testing.T) {
	reset(t)
	helper, requestLog := writeHelper(t, `{"headers":{"Authorization":["Bearer abc"]}}`, 0)
	t.Setenv(EnvVar, helper)

	require.Len(t, DialOptions("remote.example.com"), 1)
	md, err := (&rpcCredentials{uri: "grpcs://remote.example.com"}).GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, map[string]string{"authorization": "Bearer abc"}, md)
	require.Equal(t, []string{`{"uri":"grpcs://remote.example.com"}`}, requests(t, requestLog))
}
