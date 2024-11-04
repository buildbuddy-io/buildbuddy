package testenviron

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// Set sets an environment variable for the scope of the test, returning the
// variable back to its original state at the end of the test by unsetting
// or re-assigning as appropriate.
func Set(t testing.TB, name, value string) {
	originalValue, wasSet := os.LookupEnv(name)
	t.Cleanup(func() {
		if wasSet {
			os.Setenv(name, originalValue)
		} else {
			os.Unsetenv(name)
		}
	})
	err := os.Setenv(name, value)
	require.NoError(t, err)
}
