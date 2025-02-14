package storage

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

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
			previousCacheDir := os.Getenv("BUILDBUDDY_CACHE_DIR")
			err := os.Setenv("BUILDBUDDY_CACHE_DIR", t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() {
				err := os.Setenv("BUILDBUDDY_CACHE_DIR", previousCacheDir)
				require.NoError(t, err)
			})

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
