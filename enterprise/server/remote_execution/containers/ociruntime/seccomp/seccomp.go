package seccomp

import (
	"encoding/json"
	"fmt"
	"slices"

	_ "embed"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

//go:embed seccomp.json
var defaultProfileJSON []byte

// New constructs a seccomp profile from the embedded default and appends an
// allow rule for any additional syscalls configured by the executor operator.
// The configured names are removed from the default rules so that the appended
// allow rule is the only rule matching them.
func New(additionalSyscalls []string) (*specs.LinuxSeccomp, error) {
	profile := &specs.LinuxSeccomp{}
	if err := json.Unmarshal(defaultProfileJSON, profile); err != nil {
		return nil, fmt.Errorf("parse seccomp profile: %w", err)
	}
	if len(additionalSyscalls) == 0 {
		return profile, nil
	}

	names := slices.Clone(additionalSyscalls)
	slices.Sort(names)
	names = slices.Compact(names)

	// Remove the configured names from the default rules. Runtimes resolve
	// conflicting rules for the same syscall in unspecified ways, so leaving a
	// default rule in place would make the outcome unpredictable. In
	// particular, crun keeps whichever rule for a syscall comes first, which
	// would make a default deny rule silently win over the appended allow
	// rule.
	for i := range profile.Syscalls {
		rule := &profile.Syscalls[i]
		rule.Names = slices.DeleteFunc(rule.Names, func(name string) bool {
			return slices.Contains(names, name)
		})
	}
	profile.Syscalls = slices.DeleteFunc(profile.Syscalls, func(rule specs.LinuxSyscall) bool {
		return len(rule.Names) == 0
	})

	profile.Syscalls = append(profile.Syscalls, specs.LinuxSyscall{
		Names:  names,
		Action: specs.ActAllow,
	})
	return profile, nil
}
