package flags

import (
	"flag"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/config"
)

// Set a flag value and register a cleanup function to restore the flag
// to its original value after the given test is complete.
func Set(t testing.TB, name, value string) {
	config.RegisterAndParseFlags()
	f := flag.Lookup(name)
	if f == nil {
		t.Fatalf("Undefined flag: %s", name)
	}
	original := f.Value.String()
	originalSetMap := config.GetOriginalSetFlags()
	_, inOriginalSet := originalSetMap[name]
	originalSetMap[name] = struct{}{}
	flag.Set(name, value)
	t.Cleanup(func() {
		flag.Set(name, original)
		if !inOriginalSet {
			delete(originalSetMap, name)
		}
	})
}
