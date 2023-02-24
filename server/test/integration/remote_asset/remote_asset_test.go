package remote_asset_test

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/buildbuddy"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhttp"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testshell"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	testAppTarget     = flag.String("test_app_target", "", "App target to try using as the fetch backend.")
	testURL           = flag.String("test_url", "", "URL to try fetching manually.")
	testSHA256        = flag.String("test_sha256", "", "Digest of the contents at --test_url")
	testPrintContents = flag.Bool("test_print_contents", false, "Whether to print the downloaded contents.")
)

func TestRemoteAsset_ManualTest(t *testing.T) {
	if *testAppTarget == "" {
		t.Skip()
	}

	fetchedFilePath, res := fetchFileWithBazel(t, *testAppTarget, []string{*testURL}, *testSHA256)

	require.NoError(t, res.Error)
	if *testPrintContents {
		b, err := os.ReadFile(fetchedFilePath)
		require.NoError(t, err)
		fmt.Println(string(b))
	}
}

func TestRemoteAsset_OKResponse_FetchesSuccessfully(t *testing.T) {
	app := buildbuddy.Run(t)
	archiveDigest, archiveURL := serveArchive(t, map[string]string{"BUILD": ""})

	_, res := fetchArchiveWithBazel(t, app.GRPCAddress(), []string{archiveURL.String()}, archiveDigest.GetHash())

	require.NoError(t, res.Error)
}

func TestRemoteAsset_FallbackToSecondURLAfterNotFound_FetchesSuccessfully(t *testing.T) {
	app := buildbuddy.Run(t)
	archiveDigest, archiveURL := serveArchive(t, map[string]string{"BUILD": ""})
	urls := []string{
		archiveURL.String() + "_DOES_NOT_EXIST.tar.gz",
		archiveURL.String(),
	}

	_, res := fetchArchiveWithBazel(t, app.GRPCAddress(), urls, archiveDigest.GetHash())

	require.NoError(t, res.Error)
}

func TestRemoteAsset_FallbackToSecondURLAfterUnreachable_FetchesSuccessfully(t *testing.T) {
	app := buildbuddy.Run(t)
	archiveDigest, archiveURL := serveArchive(t, map[string]string{"BUILD": ""})
	urls := []string{
		"https://0.0.0.0/unreachable.tar.gz",
		archiveURL.String(),
	}

	_, res := fetchArchiveWithBazel(t, app.GRPCAddress(), urls, archiveDigest.GetHash())

	require.NoError(t, res.Error)
}

func serveArchive(t *testing.T, contents map[string]string) (*repb.Digest, *url.URL) {
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, contents)
	testshell.Run(t, ws, "tar czf archive.tar.gz $(find . -type f)")
	b, err := os.ReadFile(filepath.Join(ws, "archive.tar.gz"))
	require.NoError(t, err)
	d, err := digest.Compute(bytes.NewReader(b))
	require.NoError(t, err)
	u := testhttp.StartServer(t, http.FileServer(http.Dir(ws)))
	u.Path = "/archive.tar.gz"
	return d, u
}

func serveFile(t *testing.T, content string) (*repb.Digest, *url.URL) {
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{"file.txt": content})
	d, err := digest.Compute(strings.NewReader(content))
	require.NoError(t, err)
	u := testhttp.StartServer(t, http.FileServer(http.Dir(ws)))
	u.Path = "/file.txt"
	return d, u
}

func fetchArchiveWithBazel(t *testing.T, appTarget string, urls []string, sha256 string) (outputDir string, result *bazel.InvocationResult) {
	ws := testbazel.MakeTempWorkspace(t, map[string]string{
		"WORKSPACE": `
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
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
	ws := testbazel.MakeTempWorkspace(t, map[string]string{
		"WORKSPACE": `
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_file")
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
