package testns

import (
	"log"
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/nsutil"
)

// Unshare runs the test suite in a child process using nsutil.Unshare. See
// nsutil package docs for more details.
func Unshare(m *testing.M, opts ...nsutil.NamespaceOption) {
	child, err := nsutil.Unshare(opts...)
	if err != nil {
		log.Fatalf("unshare: %s", err)
	}
	if child != nil {
		// Wait for the child to run TestMain in the new namespace, then exit
		// with the same exit code that it exits with.
		nsutil.TerminateAfter(child)
	} else {
		code := m.Run()
		os.Exit(code)
	}
}
