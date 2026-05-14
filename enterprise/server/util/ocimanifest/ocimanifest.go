package ocimanifest

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	gcr "github.com/google/go-containerregistry/pkg/v1"
)

// FindFirstImageManifest returns the first image descriptor in the index
// manifest whose platform matches the given platform.
//
// The platform is matched if
//   - architecture and OS are identical.
//   - OS version and variant are identical if provided.
//   - features and OS features of the required platform are subsets of those of
//     the given platform.
func FindFirstImageManifest(indexManifest gcr.IndexManifest, platform gcr.Platform) (*gcr.Descriptor, error) {
	matcher := platformMatcher(platform)
	for _, manifest := range indexManifest.Manifests {
		if !manifest.MediaType.IsImage() {
			continue
		}
		if matcher(manifest) {
			return &manifest, nil
		}
	}
	return nil, status.NotFoundError("Could not find image manifest for platform")
}

// platformMatcher creates Matcher that checks if the given descriptors' platform matches the required platforms.
//
// Adapted from matchesPlatform in https://github.com/google/go-containerregistry/blob/v0.20.3/pkg/v1/remote/index.go
func platformMatcher(required gcr.Platform) func(gcr.Descriptor) bool {
	return func(desc gcr.Descriptor) bool {
		given := desc.Platform
		// Required fields that must be identical.
		if given.Architecture != required.Architecture || given.OS != required.OS {
			return false
		}

		// Optional fields that may be empty, but must be identical if provided.
		if required.OSVersion != "" && given.OSVersion != required.OSVersion {
			return false
		}
		if required.Variant != "" && given.Variant != required.Variant {
			return false
		}

		// Verify required platform's features are a subset of given platform's features.
		if !isSubset(given.OSFeatures, required.OSFeatures) {
			return false
		}
		if !isSubset(given.Features, required.Features) {
			return false
		}

		return true
	}
}

// isSubset checks if the required array of strings is a subset of the given lst.
//
// Copied from https://github.com/google/go-containerregistry/blob/v0.20.3/pkg/v1/remote/index.go
func isSubset(lst, required []string) bool {
	set := make(map[string]bool)
	for _, value := range lst {
		set[value] = true
	}

	for _, value := range required {
		if _, ok := set[value]; !ok {
			return false
		}
	}

	return true
}
