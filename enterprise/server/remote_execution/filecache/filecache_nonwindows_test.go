//go:build !windows

package filecache_test

import "testing"

func canCreateSharedDirectoryStartupFixture(name string) bool {
	return true
}

func requireFilecacheDurabilitySupport(t testing.TB, path string) {}

func requireFilecacheRestartScanSupport(t testing.TB) {}
