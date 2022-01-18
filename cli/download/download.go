package download

import (
	"context"
	"flag"
	"io"
	"net/http"
	"os"

	bblog "github.com/buildbuddy-io/buildbuddy/cli/logging"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-github/github"
)

const (
	bbOwner = "buildbuddy-io"
	bbRepo  = "buildbuddy"
)

var (
	prerelease = flag.Bool("bb_prerelease", false, "If true, allow buildbuddy prereleases.")
)

func shouldAllowPrereleases() bool {
	return os.Getenv("BB_PRERELEASE") != "" || *prerelease
}

type Binary interface {
	Version() string
	Download(ctx context.Context, outputPath string) error
}

type binaryImpl struct {
	version     string
	downloadURL string
}

func (b *binaryImpl) Version() string { return b.version }
func (b *binaryImpl) Download(ctx context.Context, outputPath string) error {
	rsp, err := http.Get(b.downloadURL)
	if err != nil {
		return err
	}
	defer rsp.Body.Close()
	f, err := os.OpenFile(outputPath, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return status.InternalErrorf("Error writing sidecar to file: %s", err.Error())
	}
	defer f.Close()
	_, err = io.Copy(f, rsp.Body)
	return err
}

func GetLatestSidecarFromGithub(ctx context.Context, sidecarName string) (Binary, error) {
	client := github.NewClient(nil)
	releases, _, err := client.Repositories.ListReleases(ctx, bbOwner, bbRepo, nil)
	if err != nil {
		return nil, err
	}
	allowPrereleases := shouldAllowPrereleases()
	for _, release := range releases {
		if *release.Prerelease && !allowPrereleases {
			bblog.Printf("Skipping prerelease %q", *release.TagName)
			continue
		}
		for _, asset := range release.Assets {
			if *asset.Name == sidecarName {
				return &binaryImpl{
					version:     *release.TagName,
					downloadURL: *asset.BrowserDownloadURL,
				}, nil
			}
		}
	}
	return nil, status.NotFoundErrorf("No sidecar found on github matching name %q", sidecarName)
}
