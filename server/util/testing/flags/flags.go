package flags

import (
	"flag"
	"strings"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/stretchr/testify/require"

	flagyaml "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/yaml"
)

var populateFlagsOnce sync.Once

func PopulateFlagsFromData(t testing.TB, testConfigData string) {
	populateFlagsOnce.Do(func() {
		// add placeholder type for type adding by testing
		flag.VisitAll(func(flg *flag.Flag) {
			if strings.HasPrefix(flg.Name, "test.") {
				flagyaml.IgnoreFlagForYAML(flg.Name)
			}
		})
		err := flagyaml.PopulateFlagsFromData(testConfigData)
		require.NoError(t, err)
	})
}

// Set a flag value and register a cleanup function to restore the flag
// to its original value after the given test is complete.
func Set(t testing.TB, name string, value any) {
	origValue, err := flagutil.GetDereferencedValue[any](name)
	require.NoError(t, err)
	err = flagutil.SetValueForFlagName(name, value, nil, false)
	require.NoError(t, err)

	t.Cleanup(func() {
		err = flagutil.SetValueForFlagName(name, origValue, nil, false)
		require.NoError(t, err)
	})
}
