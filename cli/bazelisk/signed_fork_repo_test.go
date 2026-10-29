package bazelisk

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/bazelbuild/bazelisk/config"
	"github.com/buildbuddy-io/buildbuddy/cli/signedrelease"
	"github.com/stretchr/testify/require"
)

// fakeForkRepo "downloads" by writing a file to destDir/destFile.
type fakeForkRepo struct{}

func (fakeForkRepo) GetVersions(bazeliskHome, fork string) ([]string, error) {
	return nil, nil
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
