package commandutil_test

import (
	"context"
	"fmt"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

// Returns a python script that consumes 1 CPU core continuously for the given
// duration.
func useCPUPythonScript(dur time.Duration) string {
	return fmt.Sprintf(`
import time
end = time.time() + %f
while time.time() < end:
    pass
`, dur.Seconds())
}

// Returns a python script that uses the given amount of resident memory and
// holds onto that memory for the given duration.
func useMemPythonScript(memBytes int64, dur time.Duration) string {
	return fmt.Sprintf(`
import time
arr = b'1' * %d
time.sleep(%f)
`, memBytes, dur.Seconds())
}

func runSh(ctx context.Context, script string) *interfaces.CommandResult {
	cmd := &repb.Command{Arguments: []string{"sh", "-c", script}}
	return commandutil.Run(ctx, cmd, ".", nil /*=statsListener*/, &interfaces.Stdio{})
}

func nopStatsListener(*repb.UsageStats) {}
