//go:build !race

package race

// Enabled is true iff the race detector is enabled. This is useful for tests
// that need to be skipped when the race detector is enabled.
const Enabled = false
