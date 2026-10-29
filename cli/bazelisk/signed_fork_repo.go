package bazelisk

import (
	"fmt"
	"os"

	"github.com/bazelbuild/bazelisk/config"
	"github.com/bazelbuild/bazelisk/core"
	"github.com/bazelbuild/bazelisk/platforms"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/signedrelease"
	"github.com/buildbuddy-io/buildbuddy/cli/version"
)

const bbFork = "buildbuddy-io"

// signedForkRepo wraps bazelisk's fork repo so that bb releases (versions
// "buildbuddy-io/X" in .bazelversion) are checked against their release
// signature after download.
//
// Only fresh downloads are checked. Bazelisk serves cache hits from
// ~/.cache/bazelisk without re-verifying them, and upstream bazelisk shares
// that cache with bb, so a binary that upstream bazelisk (which doesn't check
// signatures) put there is trusted as-is.
type signedForkRepo struct {
	core.ForkRepo

	// runningVersion is the version of this bb. Relative versions such as
	// "latest" never resolve to an older bb release.
	runningVersion string

	// verify checks the file at path against the signed checksum published
	// for the release asset at assetURL.
	verify func(assetURL, path string) error
}

func newSignedForkRepo(inner core.ForkRepo) *signedForkRepo {
	return &signedForkRepo{
		ForkRepo:       inner,
		runningVersion: version.String(),
		verify: func(assetURL, path string) error {
			return signedrelease.VerifyFile(signedrelease.NewDownloadClient(), assetURL, path)
		},
	}
}

// GetVersions lists the fork's versions. Bazelisk only calls it to resolve
// relative versions such as "buildbuddy-io/latest" from the (unsigned)
// GitHub API, so for bb releases it drops versions older than
// FirstSignedVersion or this bb: otherwise an attacker could steer "latest"
// to an old unsigned release, or replay an old signed one. Explicitly pinned
// versions don't go through here.
func (r *signedForkRepo) GetVersions(bazeliskHome, fork string) ([]string, error) {
	all, err := r.ForkRepo.GetVersions(bazeliskHome, fork)
	if err != nil || fork != bbFork {
		return all, err
	}
	// A dev build's version doesn't parse, and then only FirstSignedVersion
	// applies.
	running, _ := signedrelease.ParseVersion(r.runningVersion)
	var allowed []string
	for _, v := range all {
		sv, err := signedrelease.ParseVersion(v)
		if err == nil && signedrelease.IsSigned(v) && (running == nil || !sv.LessThan(running)) {
			allowed = append(allowed, v)
		}
	}
	if len(allowed) == 0 {
		return nil, fmt.Errorf("no signed %s releases (%s or later, and not older than this bb) found", bbFork, signedrelease.FirstSignedVersion)
	}
	return allowed, nil
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
	if !signedrelease.IsSigned(version) {
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
