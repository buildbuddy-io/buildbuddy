package add

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
)

// bcrMetadataURL is where the Bazel Central Registry (the registry Bazel uses
// by default) lists the versions of each module.
const bcrMetadataURL = "https://bcr.bazel.build/modules/%s/metadata.json"

type bcrMetadata struct {
	Versions       []string          `json:"versions"`
	YankedVersions map[string]string `json:"yanked_versions"`
}

func fetchBCRMetadata(module string) (*bcrMetadata, error) {
	resp, err := http.Get(fmt.Sprintf(bcrMetadataURL, module))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("module %q not found in the Bazel Central Registry, code: %d", module, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	md := &bcrMetadata{}
	if err := json.Unmarshal(body, md); err != nil {
		return nil, fmt.Errorf("parse BCR metadata for %q: %w", module, err)
	}
	return md, nil
}

// pickBCRVersion returns the version of module to depend on: the requested
// version if one was given (it must exist in the BCR and not be yanked), or
// else the latest version that's neither a pre-release nor yanked.
func pickBCRVersion(module, requested string) (string, error) {
	md, err := fetchBCRMetadata(module)
	if err != nil {
		return "", err
	}
	return pickVersion(module, requested, md)
}

func pickVersion(module, requested string, md *bcrMetadata) (string, error) {
	if requested != "" {
		// Accept "v1.2.3" for a BCR version "1.2.3", since GitHub release
		// tags often have the "v" prefix.
		for _, v := range []string{requested, strings.TrimPrefix(requested, "v")} {
			if !slices.Contains(md.Versions, v) {
				continue
			}
			if reason, yanked := md.YankedVersions[v]; yanked {
				return "", fmt.Errorf("%s@%s has been yanked from the Bazel Central Registry: %s", module, v, reason)
			}
			return v, nil
		}
		return "", fmt.Errorf("%s@%s is not in the Bazel Central Registry", module, requested)
	}
	latest := ""
	for _, v := range md.Versions {
		if _, yanked := md.YankedVersions[v]; yanked || isPrerelease(v) {
			continue
		}
		if latest == "" || compareReleases(v, latest) > 0 {
			latest = v
		}
	}
	if latest == "" {
		return "", fmt.Errorf("%s has no stable, non-yanked version in the Bazel Central Registry", module)
	}
	return latest, nil
}

// isPrerelease reports whether a Bazel module version has a pre-release part
// (RELEASE-PRERELEASE+BUILD).
func isPrerelease(v string) bool {
	release, _, _ := strings.Cut(v, "+")
	return strings.Contains(release, "-")
}

// compareReleases compares the release parts of two Bazel module versions
// the way Bazel does: dot-separated identifiers, numeric identifiers compare
// numerically and sort before non-numeric ones, which compare lexically.
func compareReleases(a, b string) int {
	as := strings.Split(releasePart(a), ".")
	bs := strings.Split(releasePart(b), ".")
	for i := 0; i < len(as) && i < len(bs); i++ {
		an, aErr := strconv.Atoi(as[i])
		bn, bErr := strconv.Atoi(bs[i])
		switch {
		case aErr == nil && bErr == nil:
			if an != bn {
				return an - bn
			}
		case aErr == nil:
			return -1
		case bErr == nil:
			return 1
		default:
			if c := strings.Compare(as[i], bs[i]); c != 0 {
				return c
			}
		}
	}
	return len(as) - len(bs)
}

func releasePart(v string) string {
	v, _, _ = strings.Cut(v, "+")
	v, _, _ = strings.Cut(v, "-")
	return v
}
