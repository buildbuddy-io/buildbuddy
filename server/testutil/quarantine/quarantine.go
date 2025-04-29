package quarantine

import (
	"os"
	"testing"
)

func SkipQuarantinedTest(t *testing.T) {
	if os.Getenv("RUN_QUARANTINED_TESTS") != "true" {
		t.Skip("skipping quarantined test")
	}
}
