package procstats_test

import (
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/procstats"
	"github.com/stretchr/testify/require"
)

func TestTreeStat(t *testing.T) {
	ts := procstats.NewTreeStats(os.Getpid())
	require.NoError(t, ts.Update())
}
