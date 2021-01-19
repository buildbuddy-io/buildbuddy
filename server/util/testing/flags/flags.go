package flags

import (
	"flag"
	"testing"
)

// Set a flag value and register a cleanup function to restore the flag
// to its original value after the given test is complete.
func Set(t *testing.T, name, value string) {
	f := flag.Lookup(name)
	if f == nil {
		t.Fatalf("Undefined flag: %s", name)
	}
	original := f.Value.String()
	flag.Set(name, value)
	t.Cleanup(func() {
		flag.Set(name, original)
	})
}
