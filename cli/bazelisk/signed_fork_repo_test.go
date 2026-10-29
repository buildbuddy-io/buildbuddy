package bazelisk

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/bazelbuild/bazelisk/config"
	"github.com/bazelbuild/bazelisk/core"
	"github.com/buildbuddy-io/buildbuddy/cli/signedrelease"
	"github.com/stretchr/testify/require"
)

// fakeForkRepo lists versions and "downloads" by writing a file to
// destDir/destFile.
type fakeForkRepo struct {
	versions []string
}

func (f fakeForkRepo) GetVersions(bazeliskHome, fork string) ([]string, error) {
	return f.versions, nil
}

func (fakeForkRepo) DownloadVersion(fork, version, destDir, destFile string, config config.Config) (string, error) {
	p := filepath.Join(destDir, destFile)
	return p, os.WriteFile(p, []byte("bb"), 0755)
}

func TestSignedForkRepo(t *testing.T) {
	arch := runtime.GOARCH
	if arch == "amd64" {
		arch = "x86_64"
	}
	for _, tc := range []struct {
		name       string
		fork       string
		version    string
		verifyErr  error
		wantVerify bool
		wantError  string
	}{
		{name: "SignedVersionVerified", fork: "buildbuddy-io", version: signedrelease.FirstSignedVersion, wantVerify: true},
		{name: "NewerVersionVerified", fork: "buildbuddy-io", version: "6.0.0", wantVerify: true},
		{name: "VerificationFails", fork: "buildbuddy-io", version: "6.0.0", verifyErr: errors.New("invalid signature"), wantVerify: true, wantError: "invalid signature"},
		{name: "OlderVersionNotVerified", fork: "buildbuddy-io", version: "5.0.498"},
		{name: "OtherForkNotVerified", fork: "someone-else", version: "6.0.0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gotURL, gotPath string
			r := &signedForkRepo{
				ForkRepo: fakeForkRepo{},
				verify: func(assetURL, path string) error {
					gotURL, gotPath = assetURL, path
					return tc.verifyErr
				},
			}
			dir := t.TempDir()

			p, err := r.DownloadVersion(tc.fork, tc.version, dir, "bazel", config.Null())

			dest := filepath.Join(dir, "bazel")
			if tc.wantVerify {
				require.Equal(t, signedrelease.ReleaseDownloadBaseURL+"/"+tc.version+"/bazel-"+tc.version+"-"+runtime.GOOS+"-"+arch, gotURL)
				require.Equal(t, dest, gotPath)
			} else {
				require.Empty(t, gotURL)
			}
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				require.NoFileExists(t, dest)
				return
			}
			require.NoError(t, err)
			require.Equal(t, dest, p)
			require.FileExists(t, dest)
		})
	}
}

func TestSignedForkRepoLatest(t *testing.T) {
	for _, tc := range []struct {
		name      string
		fork      string
		running   string
		listed    []string
		want      string
		wantError string
	}{
		// A signed release older than this bb can't be replayed as "latest".
		{name: "OlderThanRunningDropped", fork: "buildbuddy-io", running: "5.0.501", listed: []string{"5.0.499", "5.0.500"}, wantError: "not older than this bb"},
		{name: "RunningVersionAllowed", fork: "buildbuddy-io", running: "5.0.500", listed: []string{"5.0.499", "5.0.500"}, want: "5.0.500"},
		{name: "DevBuildUsesFirstSigned", fork: "buildbuddy-io", running: "unknown", listed: []string{"5.0.498", "5.0.499"}, want: "5.0.499"},
		{name: "LatestIsSigned", fork: "buildbuddy-io", listed: []string{"5.0.498", "5.0.499", "5.0.500"}, want: "5.0.500"},
		// An attacker serving the unsigned listing can't steer "latest" to
		// an old, unsigned release.
		{name: "OnlyUnsignedListed", fork: "buildbuddy-io", listed: []string{"5.0.497", "5.0.498"}, wantError: "no signed buildbuddy-io releases"},
		{name: "UnsignedDroppedFromListing", fork: "buildbuddy-io", listed: []string{"5.0.499", "4.0.0", "garbage"}, want: "5.0.499"},
		{name: "OtherForkUnfiltered", fork: "someone-else", listed: []string{"1.0.0", "2.0.0"}, want: "2.0.0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var verified []string
			r := &signedForkRepo{
				ForkRepo:       fakeForkRepo{versions: tc.listed},
				runningVersion: tc.running,
				verify: func(assetURL, path string) error {
					verified = append(verified, assetURL)
					return nil
				},
			}
			repos := core.CreateRepositories(nil, r, nil, nil, false)

			v, download, err := repos.ResolveVersion(t.TempDir(), tc.fork, "latest", config.Null())

			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, v)
			_, err = download(t.TempDir(), "bazel")
			require.NoError(t, err)
			if tc.fork == "buildbuddy-io" {
				require.Len(t, verified, 1)
			} else {
				require.Empty(t, verified)
			}
		})
	}
}
