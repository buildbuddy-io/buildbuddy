// Package rc_util contains helpers shared by rc file formats.
package rc_util

const (
	CommonPhase = "common"
	AlwaysPhase = "always"
)

// IsUnconditionalCommandPhase returns whether phase is evaluated regardless
// of the command.
func IsUnconditionalCommandPhase(phase string) bool {
	return phase == CommonPhase || phase == AlwaysPhase
}
