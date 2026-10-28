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

// New constructs a seccomp profile from the embedded default and appends an
// allow rule for any additional syscalls configured by the executor operator.
// The configured names are removed from the default rules so that the appended
// allow rule is the only rule matching them.
//
// If disableNewUserNS is true and capabilities does not include CAP_SYS_ADMIN,
// the profile also prevents creating user namespaces, except through syscalls
// listed in additionalSyscalls.
func New(additionalSyscalls, capabilities []string, disableNewUserNS bool) (*specs.LinuxSeccomp, error) {
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
	// To match Docker's behavior, containers with CAP_SYS_ADMIN may create
	// user namespaces.
	if disableNewUserNS && !slices.Contains(capabilities, "CAP_SYS_ADMIN") {
		// The embedded profile allows clone, clone3, and unshare with any
		// arguments. clone3 passes its flags in a struct that seccomp cannot
		// inspect, so we remove it from the allow list. It then gets the
		// default action, which returns ENOSYS, and libc falls back to clone.
		removeSyscalls(profile, []string{"clone", "clone3", "unshare"})
		// Block clone and unshare if CLONE_NEWUSER is passed as an arg since
		// this grants new capabilities within the user namespace.
		for _, name := range []string{"clone", "unshare"} {
			argIndex := uint(0)
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
	}

	if len(additionalSyscalls) == 0 {
		return profile, nil
	}

	names := slices.Clone(additionalSyscalls)
	slices.Sort(names)
	names = slices.Compact(names)

	// Remove the configured names from the existing rules. Runtimes resolve
	// conflicting rules for the same syscall in unspecified ways, so leaving an
	// existing rule in place would make the outcome unpredictable. In
	// particular, crun keeps whichever rule for a syscall comes first, which
	// would make a default deny rule silently win over the appended allow
	// rule.
	removeSyscalls(profile, names)

	profile.Syscalls = append(profile.Syscalls, specs.LinuxSyscall{
		Names:  names,
		Action: specs.ActAllow,
	})
	return profile, nil
}

// removeSyscalls removes names from the profile's rules, and drops the rules
// that no longer name any syscall.
func removeSyscalls(profile *specs.LinuxSeccomp, names []string) {
	for i := range profile.Syscalls {
		rule := &profile.Syscalls[i]
		rule.Names = slices.DeleteFunc(rule.Names, func(name string) bool {
			return slices.Contains(names, name)
		})
	}
	profile.Syscalls = slices.DeleteFunc(profile.Syscalls, func(rule specs.LinuxSyscall) bool {
		return len(rule.Names) == 0
	})
}
