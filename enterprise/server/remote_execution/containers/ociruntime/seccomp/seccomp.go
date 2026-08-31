package seccomp

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"slices"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

//go:embed seccomp.json
var defaultProfileJSON []byte

// New constructs a seccomp profile from the embedded default and appends an
// allow rule for any additional syscalls configured by the executor operator.
func New(additionalSyscalls []string) (*specs.LinuxSeccomp, error) {
	profile := &specs.LinuxSeccomp{}
	if err := json.Unmarshal(defaultProfileJSON, profile); err != nil {
		return nil, fmt.Errorf("parse seccomp profile: %w", err)
	}

	if len(additionalSyscalls) > 0 {
		names := slices.Clone(additionalSyscalls)
		slices.Sort(names)
		names = slices.Compact(names)
		profile.Syscalls = append(profile.Syscalls, specs.LinuxSyscall{
			Names:  names,
			Action: specs.ActAllow,
		})
	}

	return profile, nil
}
