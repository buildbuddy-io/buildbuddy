package timeout_fixture_test

import (
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/tools/test_buddy/timeout_fixture"
)

func TestAlwaysTimesOut(t *testing.T) {
	time.Sleep(timeout_fixture.SleepDuration())
}
