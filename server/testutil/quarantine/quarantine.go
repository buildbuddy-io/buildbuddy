package quarantine

import (
	"os"
	"testing"
)

// SkipQuarantinedTest skips the current test unless
// the RUN_QUARANTINED_TESTS environment variable is set to "true".
func SkipQuarantinedTest(t *testing.T) {
	if os.Getenv("RUN_QUARANTINED_TESTS") != "true" {
		t.Skip("skipping quarantined test")
	}
}
