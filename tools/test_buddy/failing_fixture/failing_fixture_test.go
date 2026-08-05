package failing_fixture_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/tools/test_buddy/failing_fixture"
)

func TestAlwaysFails(t *testing.T) {
	t.Fatal(failing_fixture.FailureMessage())
}
