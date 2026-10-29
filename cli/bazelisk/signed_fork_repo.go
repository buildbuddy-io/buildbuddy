package bazelisk

import (
	"fmt"
	"os"

	"github.com/bazelbuild/bazelisk/config"
	"github.com/bazelbuild/bazelisk/core"
	"github.com/bazelbuild/bazelisk/platforms"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/signedrelease"
)

const bbFork = "buildbuddy-io"

// signedForkRepo wraps bazelisk's fork repo so that bb releases (versions
// "buildbuddy-io/X" in .bazelversion) are checked against their release
// signature after download.
type signedForkRepo struct {
	core.ForkRepo

	// verify checks the file at path against the signed checksum published
	// for the release asset at assetURL.
	verify func(assetURL, path string) error
}

func newSignedForkRepo(inner core.ForkRepo) *signedForkRepo {
	return &signedForkRepo{
		ForkRepo: inner,
		verify: func(assetURL, path string) error {
			return signedrelease.VerifyFile(signedrelease.NewDownloadClient(), assetURL, path)
		},
	}
}

func (r *signedForkRepo) DownloadVersion(fork, version, destDir, destFile string, config config.Config) (string, error) {
	p, err := r.ForkRepo.DownloadVersion(fork, version, destDir, destFile, config)
	if err != nil {
		return "", err
	}
	if fork != bbFork {
		log.Warnf("Not verifying the signature of %s/%s: only %s releases are signed.", fork, version, bbFork)
		return p, nil
	}
	if cmp, err := signedrelease.CompareVersions(version, signedrelease.FirstSignedVersion); err != nil || cmp < 0 {
		log.Warnf("Not verifying the signature of %s/%s: only versions %s and later are signed.", fork, version, signedrelease.FirstSignedVersion)
		return p, nil
	}
	filename, err := platforms.DetermineBazelFilename(version, true, config)
	if err == nil {
		err = r.verify(fmt.Sprintf("%s/%s/%s", signedrelease.ReleaseDownloadBaseURL, version, filename), p)
	}
	if err != nil {
		os.Remove(p)
		return "", fmt.Errorf("verify %s/%s: %w", fork, version, err)
	}
	return p, nil
}
