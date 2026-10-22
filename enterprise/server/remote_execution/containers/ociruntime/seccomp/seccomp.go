package seccomp

import (
	"encoding/json"
	"fmt"
	"runtime"
	"slices"

	_ "embed"

	"golang.org/x/sys/unix"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

//go:embed seccomp.json
var defaultProfileJSON []byte

// New constructs a seccomp profile from the embedded default, restricting user
// namespace creation and allowing clone, clone3, and unshare when CAP_SYS_ADMIN
// is granted. Additional syscalls override all restrictions on those names.
func New(additionalSyscalls, capabilities []string) (*specs.LinuxSeccomp, error) {
	// The embedded profile uses Docker's archMap field. OCI expects an
	// architectures list containing the native architecture and its compatible
	// sub-architectures. Select only the entry for this executor's architecture.
	dockerProfile := &struct {
		specs.LinuxSeccomp
		ArchMap []struct {
			Architecture     specs.Arch   `json:"architecture"`
			SubArchitectures []specs.Arch `json:"subArchitectures"`
		} `json:"archMap"`
	}{}
	if err := json.Unmarshal(defaultProfileJSON, dockerProfile); err != nil {
		return nil, fmt.Errorf("parse seccomp profile: %w", err)
	}
	profile := &dockerProfile.LinuxSeccomp
	// These are the native architectures with compatibility entries in the
	// embedded profile. Other architectures retain OCI's native-only default.
	nativeArch, ok := map[string]specs.Arch{
		"amd64":    specs.ArchX86_64,
		"arm64":    specs.ArchAARCH64,
		"mips64":   specs.ArchMIPS64,
		"mips64le": specs.ArchMIPSEL64,
		"s390x":    specs.ArchS390X,
	}[runtime.GOARCH]
	if ok {
		for _, mapping := range dockerProfile.ArchMap {
			if mapping.Architecture == nativeArch {
				profile.Architectures = append([]specs.Arch{mapping.Architecture}, mapping.SubArchitectures...)
				break
			}
		}
	}
	// Block clone and unshare if CLONE_NEWUSER is passed as an arg since this
	// grants new capabilities within the user namespace.
	for _, name := range []string{"clone", "unshare"} {
		argIndex := uint(0)
		if name == "clone" && runtime.GOARCH == "s390x" {
			argIndex = 1
		}
		profile.Syscalls = append(profile.Syscalls, specs.LinuxSyscall{
			Names:  []string{name},
			Action: specs.ActAllow,
			Args: []specs.LinuxSeccompArg{{
				Index: argIndex,
				Value: unix.CLONE_NEWUSER,
				Op:    specs.OpMaskedEqual,
			}},
		}, specs.LinuxSyscall{
			Names:    []string{name},
			Action:   specs.ActErrno,
			ErrnoRet: new(uint(unix.EPERM)),
			Args: []specs.LinuxSeccompArg{{
				Index:    argIndex,
				Value:    unix.CLONE_NEWUSER,
				ValueTwo: unix.CLONE_NEWUSER,
				Op:       specs.OpMaskedEqual,
			}},
		})
	}

	names := slices.Clone(additionalSyscalls)

	// To match Docker's behavior, allow clone, clone3, and unshare if
	// CAP_SYS_ADMIN is granted.
	if slices.Contains(capabilities, "CAP_SYS_ADMIN") {
		names = append(names, "clone", "clone3", "unshare")
	}
	if len(names) == 0 {
		return profile, nil
	}
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
