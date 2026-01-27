package image_verification_test

import (
	"flag"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"

)

// Set via x_defs in BUILD file.
var (
	executorImageRlocationpath string
)

var (
	testExecutorRoot = flag.String("test_executor_root", "/tmp/test-executor-root", "If set, use this as the executor root data dir. Helps avoid excessive image pulling when re-running tests.")
)

func serveExecutorImage(t *testing.T) string {
	registry := testregistry.Run(t, testregistry.Opts{})
	image := testregistry.ImageFromRlocationpath(t, executorImageRlocationpath)
	imageName := "bb-executor"
	registry.Push(t, image, imageName, nil)
	return registry.ImageAddress(imageName)
}
