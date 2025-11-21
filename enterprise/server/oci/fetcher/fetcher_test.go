package fetcher_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/fetcher"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/stretchr/testify/require"
)

func TestRegister(t *testing.T) {
	env := testenv.GetTestEnv(t)
	err := fetcher.Register(env)
	require.NoError(t, err)
}
