package testcontainer

import "github.com/buildbuddy-io/buildbuddy/server/interfaces"

type testImageCacheAuthenticator struct{}

// PermissiveCacheAuthenticator returns an ImageCacheAuthenticator that always
// returns true for IsValid, so that tests which have images already cached do
// not need to re-authenticate with the remote registry.
func PermissiveImageCacheAuthenticator() *testImageCacheAuthenticator {
	return &testImageCacheAuthenticator{}
}

func (*testImageCacheAuthenticator) IsAuthorized(_ interfaces.ImageCacheToken) bool {
	return true
}

func (*testImageCacheAuthenticator) Refresh(_ interfaces.ImageCacheToken) {}
