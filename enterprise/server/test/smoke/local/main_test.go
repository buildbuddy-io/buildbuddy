package local_test

import (
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/smoke/executorsmoke"
)

func TestMain(m *testing.M) {
	// The test binary also serves as the program run by smoke test actions.
	executorsmoke.MaybeRunTool()
	os.Exit(m.Run())
}
