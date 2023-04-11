package testrand

import (
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// Provide deteministic random numbers for tests.
// Account for Bazel setting the TEST_RANDOM_SEED environment variable when
// --runs_per_test=n is used.
func NewRandom(t *testing.T) *rand.Rand {
	seed := int64(0)
	if bazelRandSeed := os.Getenv("TEST_RANDOM_SEED"); bazelRandSeed != "" {
		// TEST_RANDOM_SEED is set by Bazel when --runs_per_test=n is used
		runSeed, err := strconv.Atoi(bazelRandSeed)
		require.NoError(t, err)

		seed += int64(runSeed)
	}
	return rand.New(rand.NewSource(seed))
}
