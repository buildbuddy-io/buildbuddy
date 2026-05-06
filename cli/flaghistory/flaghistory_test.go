package flaghistory

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T) {
	// Flag history is written to this dir.
	t.Setenv("BUILDBUDDY_CACHE_DIR", t.TempDir())
}

func TestMain(m *testing.M) {
	// Change to a directory that looks like a Bazel workspace so that
	// workspace.Path() can find it.
	workspaceDir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		panic(err)
	}
	if err = os.Chdir(workspaceDir); err != nil {
		panic(err)
	}
	if err = os.WriteFile("MODULE.bazel", []byte{}, 0644); err != nil {
		panic(err)
	}

	exitCode := m.Run()
	_ = os.RemoveAll(workspaceDir)
	os.Exit(exitCode)
}

func TestPreviousFlagStorage(t *testing.T) {
	for maxValues := 1; maxValues <= 3; maxValues++ {
		t.Run("maxValues="+strconv.Itoa(maxValues), func(t *testing.T) {
			setup(t)

			flag := "flag" + strconv.Itoa(maxValues)
			rng := rand.New(rand.NewSource(int64(maxValues)))

			for numStores := 0; numStores <= 3*maxValues; numStores++ {
				var expectedValues []string
				for i := numStores; i > 0 && i > numStores-maxValues; i-- {
					expectedValues = append(expectedValues, fmt.Sprintf("value_%d", i))
				}
				for len(expectedValues) < maxValues {
					expectedValues = append(expectedValues, "")
				}

				var actualValues []string
				for i := 1; i <= maxValues; i++ {
					v, err := GetNthPreviousFlag(flag, i)
					require.NoError(t, err)
					actualValues = append(actualValues, v)
				}

				require.Equalf(t, expectedValues, actualValues, "numStores=%d", numStores)

				value := fmt.Sprintf("value_%d", numStores+1)
				flagAndValue := fmt.Sprintf("--%s=%s", flag, value)
				var argsIn []string
				var backup string
				// Deterministically vary whether the flag is specified explicitly.
				if rng.Int()%2 == 0 {
					argsIn = []string{flagAndValue}
					backup = ""
				} else {
					argsIn = []string{}
					backup = value
				}
				argsOut := saveFlag(argsIn, flag, backup, maxValues)
				require.Equal(t, flagAndValue, argsOut[len(argsOut)-1])
			}
		})
	}

}

func TestSaveFlags(t *testing.T) {
	setup(t)

	args := SaveFlags([]string{
		"build",
		"--bes_backend=grpc://backend.example",
		"--bes_results_url=https://app.buildbuddy.io/invocation/abc",
		"--invocation_id=explicit-invocation-id",
		"//foo",
	})

	require.Equal(t, "explicit-invocation-id", arg.Get(args, InvocationIDFlagName))
	requirePreviousFlag(t, besBackendFlagName, "grpc://backend.example")
	requirePreviousFlag(t, BesResultsUrlFlagName, "https://app.buildbuddy.io/invocation/abc")
	requirePreviousFlag(t, InvocationIDFlagName, "explicit-invocation-id")
}

func TestSaveFlags_ClearsStaleHistory(t *testing.T) {
	setup(t)

	// Save some flags originally.
	SaveFlags([]string{
		"build",
		"--bes_backend=grpc://backend.example",
		"--bes_results_url=https://app.buildbuddy.io/invocation/abc",
		"--invocation_id=explicit-invocation-id",
		"//foo",
	})
	requirePreviousFlag(t, besBackendFlagName, "grpc://backend.example")
	requirePreviousFlag(t, BesResultsUrlFlagName, "https://app.buildbuddy.io/invocation/abc")
	requirePreviousFlag(t, InvocationIDFlagName, "explicit-invocation-id")

	// Run another build that does not set --bes_backend or --bes_results_url.
	// This should clear the old flags.
	args := SaveFlags([]string{"build", "//foo"})

	requirePreviousFlag(t, besBackendFlagName, "")
	requirePreviousFlag(t, BesResultsUrlFlagName, "")
	// The invocation ID should be set to an automatically generated one.
	// It should not be the value that was saved from the original run.
	invocationID := arg.Get(args, InvocationIDFlagName)
	require.NotEmpty(t, invocationID)
	require.NotEqual(t, "explicit-invocation-id", invocationID)
	requirePreviousFlag(t, InvocationIDFlagName, invocationID)
}

func TestSaveFlags_NonBazelCommands(t *testing.T) {
	setup(t)

	// Save some bazel flags.
	SaveFlags([]string{
		"build",
		"--bes_backend=grpc://backend.example",
		"//foo",
	})

	// Run another command that doesn't support flag history.
	args := SaveFlags([]string{"install", "--bes_backend=shouldnt_apply"})

	require.Equal(t, []string{"install", "--bes_backend=shouldnt_apply"}, args)
	requirePreviousFlag(t, besBackendFlagName, "grpc://backend.example")
}

func requirePreviousFlag(t *testing.T, flagName, expected string) {
	actual, err := GetPreviousFlag(flagName)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}
