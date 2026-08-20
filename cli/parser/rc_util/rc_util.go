// Package rc_util contains helpers shared by rc file formats.
package rc_util

import "iter"

const (
	CommonPhase = "common"
	AlwaysPhase = "always"
)

func UnconditionalCommandPhases() iter.Seq[string] {
	return func(yield func(string) bool) {
		if !yield(CommonPhase) {
			return
		}
		if !yield(AlwaysPhase) {
			return
		}
	}
}

// IsUnconditionalCommandPhase returns whether phase is evaluated regardless
// of the command.
func IsUnconditionalCommandPhase(phase string) bool {
	return phase == CommonPhase || phase == AlwaysPhase
}
