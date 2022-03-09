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
	flag.Set(name, value)
	wasSet := config.TestOnlySetFlag(name, true)
	t.Cleanup(func() {
		flag.Set(name, original)
		config.TestOnlySetFlag(name, wasSet)
	})
}
