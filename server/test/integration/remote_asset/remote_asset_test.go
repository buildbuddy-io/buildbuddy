package remote_asset_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
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

func TestRemoteAsset_OKResponse_FetchesSuccessfully(t *testing.T) {
	app := buildbuddy.Run(t)
	archiveDigest, archiveURL := serveArchive(t, map[string]string{"BUILD": ""})

	res := fetchWithBazel(t, app.GRPCAddress(), []string{archiveURL.String()}, archiveDigest.GetHash())

	require.NoError(t, res.Error)
}

func TestRemoteAsset_FallbackToSecondURLAfterNotFound_FetchesSuccessfully(t *testing.T) {
	app := buildbuddy.Run(t)
	archiveDigest, archiveURL := serveArchive(t, map[string]string{"BUILD": ""})
	urls := []string{
		archiveURL.String() + "_DOES_NOT_EXIST.tar.gz",
		archiveURL.String(),
	}

	res := fetchWithBazel(t, app.GRPCAddress(), urls, archiveDigest.GetHash())

	require.NoError(t, res.Error)
}

func TestRemoteAsset_FallbackToSecondURLAfterUnreachable_FetchesSuccessfully(t *testing.T) {
	app := buildbuddy.Run(t)
	archiveDigest, archiveURL := serveArchive(t, map[string]string{"BUILD": ""})
	urls := []string{
		"https://0.0.0.0/unreachable.tar.gz",
		archiveURL.String(),
	}

	res := fetchWithBazel(t, app.GRPCAddress(), urls, archiveDigest.GetHash())

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

func fetchWithBazel(t *testing.T, appTarget string, urls []string, sha256 string) *bazel.InvocationResult {
	urlsJSON := &bytes.Buffer{}
	err := json.NewEncoder(urlsJSON).Encode(urls)
	require.NoError(t, err)

	ws := testbazel.MakeTempWorkspace(t, map[string]string{
		"WORKSPACE": `
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
http_archive(
    name = "test",
    sha256 = "` + sha256 + `",
    urls = ` + urlsJSON.String() + `,
)
`,
	})
	return testbazel.Invoke(
		context.Background(), t, ws,
		"fetch", "@test//...",
		"--experimental_remote_downloader="+appTarget,
		"--remote_cache="+appTarget)
}
