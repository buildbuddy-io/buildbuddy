//go:build test
// +build test

package action_cache_server

func TestOnly_SetRestrictedPrefixes(testPrefixes []string) {
	restrictedPrefixes = testPrefixes
}
