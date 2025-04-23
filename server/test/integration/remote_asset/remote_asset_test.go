package remote_asset_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/app"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/buildbuddy"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhttp"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestRemoteAsset_OKResponse_FetchesSuccessfully(t *testing.T) {
	app := runApp(t)
	archiveDigest, archiveURL := serveArchive(t, map[string]string{"BUILD": ""})

	_, res := fetchArchiveWithBazel(t, app.GRPCAddress(), []string{archiveURL.String()}, archiveDigest.GetHash())

	require.NoError(t, res.Error)
}

func TestRemoteAsset_FallbackToSecondURLAfterNotFound_FetchesSuccessfully(t *testing.T) {
	app := runApp(t)
	archiveDigest, archiveURL := serveArchive(t, map[string]string{"BUILD": ""})
	urls := []string{
		archiveURL.String() + "_DOES_NOT_EXIST.tar.gz",
		archiveURL.String(),
	}

	_, res := fetchArchiveWithBazel(t, app.GRPCAddress(), urls, archiveDigest.GetHash())

	require.NoError(t, res.Error)
}

func TestRemoteAsset_FallbackToSecondURLAfterUnreachable_FetchesSuccessfully(t *testing.T) {
	app := runApp(t)
	archiveDigest, archiveURL := serveArchive(t, map[string]string{"BUILD": ""})
	urls := []string{
		"https://0.0.0.0/unreachable.tar.gz",
		archiveURL.String(),
	}

	_, res := fetchArchiveWithBazel(t, app.GRPCAddress(), urls, archiveDigest.GetHash())

	require.NoError(t, res.Error)
}

func TestRemoteAsset_WithProxy(t *testing.T) {
	// Setup file server
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{"BUILD": ""})
	testshell.Run(t, ws, "tar czf archive.tar.gz $(find . -type f)")
	b, err := os.ReadFile(filepath.Join(ws, "archive.tar.gz"))
	require.NoError(t, err)
	d, err := digest.Compute(bytes.NewReader(b), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	fileServer := http.FileServer(http.Dir(ws))

	// setup proxy
	proxyUsed := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxyUsed = true
		fileServer.ServeHTTP(w, r)
	}))
	defer ts.Close()

	// Ensure that HTTP_PROXY is set before app starts
	defer os.Setenv("HTTP_PROXY", os.Getenv("HTTP_PROXY"))
	err = os.Setenv("HTTP_PROXY", ts.URL)
	require.NoError(t, err)
	app := runApp(t)

	_, res := fetchArchiveWithBazel(t, app.GRPCAddress(), []string{"http://some-url.does.not.exist/archive.tar.gz"}, d.GetHash())
	require.NoError(t, res.Error)
	require.True(t, proxyUsed)
}

func serveArchive(t *testing.T, contents map[string]string) (*repb.Digest, *url.URL) {
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, contents)
	testshell.Run(t, ws, "tar czf archive.tar.gz $(find . -type f)")
	b, err := os.ReadFile(filepath.Join(ws, "archive.tar.gz"))
	require.NoError(t, err)
	d, err := digest.Compute(bytes.NewReader(b), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	u := testhttp.StartServer(t, http.FileServer(http.Dir(ws)))
	u.Path = "/archive.tar.gz"
	return d, u
}

func fetchArchiveWithBazel(t *testing.T, appTarget string, urls []string, sha256 string) (outputDir string, result *bazel.InvocationResult) {
	ws := testbazel.MakeTempModule(t, map[string]string{
		"MODULE.bazel": `
http_archive = use_repo_rule("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
http_archive(
    name = "test",
    ` + starlarkOptionalSHA256Kwarg(sha256) + `
    urls = ` + starlarkStringListRepr(urls) + `,
)
`,
	})
	result = testbazel.Invoke(
		context.Background(), t, ws,
		"fetch", "@test//...",
		"--experimental_remote_downloader="+appTarget,
		"--remote_cache="+appTarget)
	outputDir = filepath.Join(ws, fmt.Sprintf("bazel-%s", filepath.Dir(ws)), "external/test")
	return outputDir, result
}

func fetchFileWithBazel(t *testing.T, appTarget string, urls []string, sha256 string) (outputPath string, result *bazel.InvocationResult) {
	ws := testbazel.MakeTempModule(t, map[string]string{
		"MODULE.bazel": `
http_file = use_repo_rule("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
http_file(
    name = "test",
    ` + starlarkOptionalSHA256Kwarg(sha256) + `
    urls = ` + starlarkStringListRepr(urls) + `,
)
`,
	})
	result = testbazel.Invoke(
		context.Background(), t, ws,
		"build", "@test//...",
		"--experimental_remote_downloader="+appTarget,
		"--remote_cache="+appTarget)
	outputPath = filepath.Join(ws, fmt.Sprintf("bazel-%s", filepath.Base(ws)), "external/test/file/downloaded")
	return outputPath, result
}

func starlarkStringListRepr(values []string) string {
	quotedValues := make([]string, 0, len(values))
	for _, v := range values {
		quotedValues = append(quotedValues, fmt.Sprintf("%q", v))
	}
	return "[" + strings.Join(quotedValues, ", ") + "]"
}

func starlarkOptionalSHA256Kwarg(sha256 string) string {
	if sha256 == "" {
		return ""
	}
	return fmt.Sprintf("sha256 = %q,", sha256)
}

func runApp(t *testing.T) *app.App {
	// The test server we're fetching from runs locally, so we need to allow
	// the loopback range for this test.
	return buildbuddy.Run(t, "--remote_asset.allowed_private_ips=127.0.0.0/8")
}
