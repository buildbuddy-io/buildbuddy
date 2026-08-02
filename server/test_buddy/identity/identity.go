// Package identity validates TestBuddy target and case addresses.
package identity

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	gazellelabel "github.com/bazelbuild/bazel-gazelle/label"
	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	MaxRepositoryURLBytes = 512
	MaxPackagePathBytes   = 1024
	MaxTargetNameBytes    = 512
	MaxTargetLabelBytes   = MaxPackagePathBytes + MaxTargetNameBytes + len("//:")
	MaxCaseNameBytes      = 512
)

type PackageAddress struct {
	Repository  string
	PackagePath string
}

func (a PackageAddress) Validate() error {
	repository, err := NormalizeRepositoryURL(a.Repository)
	if err != nil {
		return err
	}
	if repository != a.Repository {
		return status.InvalidArgumentError("repository URL is not canonical")
	}
	packagePath, _, err := parseTargetLabel("//" + a.PackagePath + ":__test_buddy_package__")
	if err != nil {
		return err
	}
	if packagePath != a.PackagePath {
		return status.InvalidArgumentError("package path is not canonical")
	}
	return nil
}

type TargetAddress struct {
	Repository  string
	PackagePath string
	TargetName  string
}

func (a TargetAddress) Package() PackageAddress {
	return PackageAddress{Repository: a.Repository, PackagePath: a.PackagePath}
}

func (a TargetAddress) Validate() error {
	if err := a.Package().Validate(); err != nil {
		return err
	}
	packagePath, targetName, err := parseTargetLabel(a.Label())
	if err != nil {
		return err
	}
	if packagePath != a.PackagePath || targetName != a.TargetName {
		return status.InvalidArgumentError("target address is not canonical")
	}
	return nil
}

func (a TargetAddress) Label() string {
	return "//" + a.PackagePath + ":" + a.TargetName
}

func (a TargetAddress) String() string {
	return fmt.Sprintf("%s:%s:%s", strconv.Quote(a.Repository),
		strconv.Quote(a.PackagePath), strconv.Quote(a.TargetName))
}

func (a TargetAddress) IsZero() bool { return a == TargetAddress{} }

type CaseAddress struct {
	TargetAddress
	CaseName string
}

func (a CaseAddress) Target() TargetAddress { return a.TargetAddress }

func (a CaseAddress) Validate() error {
	if err := a.Target().Validate(); err != nil {
		return err
	}
	return ValidateCaseName(a.CaseName)
}

func (a CaseAddress) String() string {
	return fmt.Sprintf("%s:%s", a.TargetAddress.String(), strconv.Quote(a.CaseName))
}

func (a CaseAddress) IsZero() bool { return a == CaseAddress{} }

func CanonicalizeCase(repositoryURL, targetLabel, caseName string) (CaseAddress, error) {
	target, err := CanonicalizeTarget(repositoryURL, targetLabel)
	if err != nil {
		return CaseAddress{}, err
	}
	if err := ValidateCaseName(caseName); err != nil {
		return CaseAddress{}, err
	}
	return CaseAddress{TargetAddress: target, CaseName: caseName}, nil
}

func CanonicalizeTarget(repositoryURL, targetLabel string) (TargetAddress, error) {
	repository, err := NormalizeRepositoryURL(repositoryURL)
	if err != nil {
		return TargetAddress{}, err
	}
	packagePath, targetName, err := parseTargetLabel(targetLabel)
	if err != nil {
		return TargetAddress{}, err
	}
	return TargetAddress{
		Repository: repository, PackagePath: packagePath, TargetName: targetName,
	}, nil
}

func CanonicalizeTargetLabel(targetLabel string) (string, error) {
	packagePath, targetName, err := parseTargetLabel(targetLabel)
	if err != nil {
		return "", err
	}
	return TargetAddress{PackagePath: packagePath, TargetName: targetName}.Label(), nil
}

func CanonicalizePackagePath(packagePath string) (string, error) {
	canonical, _, err := parseTargetLabel("//" + packagePath + ":__test_buddy_package__")
	return canonical, err
}

func parseTargetLabel(raw string) (string, string, error) {
	if raw == "" {
		return "", "", status.InvalidArgumentError("target label is required")
	}
	if err := validatePrintableASCII("target label", raw, MaxTargetLabelBytes); err != nil {
		return "", "", err
	}
	parsed, err := gazellelabel.Parse(raw)
	if err != nil {
		return "", "", status.InvalidArgumentErrorf("invalid target label %q: %s", raw, err)
	}
	if parsed.Relative {
		return "", "", status.InvalidArgumentErrorf("target label %q must be absolute", raw)
	}
	if parsed.Repo != "" && parsed.Repo != "@" {
		return "", "", status.InvalidArgumentErrorf(
			"external repository target label %q is not supported", raw)
	}
	if len(parsed.Pkg) > MaxPackagePathBytes {
		return "", "", status.InvalidArgumentErrorf(
			"package path exceeds %d bytes", MaxPackagePathBytes)
	}
	if len(parsed.Name) > MaxTargetNameBytes {
		return "", "", status.InvalidArgumentErrorf(
			"target name exceeds %d bytes", MaxTargetNameBytes)
	}
	return parsed.Pkg, parsed.Name, nil
}

func (a CaseAddress) Proto() *tbpb.TestCaseIdentity { return CaseProto(a) }

func (a TargetAddress) Proto() *tbpb.TestTargetIdentity { return TargetProto(a) }

func CaseProto(address CaseAddress) *tbpb.TestCaseIdentity {
	return &tbpb.TestCaseIdentity{
		Target: TargetProto(address.Target()), CaseName: address.CaseName,
	}
}

func TargetProto(address TargetAddress) *tbpb.TestTargetIdentity {
	return &tbpb.TestTargetIdentity{TargetLabel: address.Label()}
}

func CaseAddressFromProto(repository string, in *tbpb.TestCaseIdentity) (CaseAddress, error) {
	return CanonicalizeCase(repository, in.GetTarget().GetTargetLabel(), in.GetCaseName())
}

func NormalizeRepositoryURL(raw string) (string, error) {
	normalized, err := gitutil.NormalizeRepoURL(raw)
	if err != nil {
		return "", status.InvalidArgumentErrorf("normalize repository URL: %s", err)
	}
	value := normalized.String()
	if value == "" {
		return "", status.InvalidArgumentError("repository URL is required")
	}
	if err := validatePrintableASCII("repository URL", value, MaxRepositoryURLBytes); err != nil {
		return "", err
	}
	return value, nil
}

func ValidateCaseName(caseName string) error {
	if caseName == "" {
		return status.InvalidArgumentError("case name is required")
	}
	return validatePrintableASCII("case name", caseName, MaxCaseNameBytes)
}

func validatePrintableASCII(name, value string, maxBytes int) error {
	if err := ValidateBoundedString(name, value, maxBytes); err != nil {
		return err
	}
	for i := 0; i < len(value); i++ {
		if c := value[i]; c < 0x20 || c > 0x7e {
			return status.InvalidArgumentErrorf(
				"%s must be printable ASCII; byte %d is %#02x", name, i, c)
		}
	}
	return nil
}

func ValidateBoundedString(name, value string, maxBytes int) error {
	if len(value) > maxBytes {
		return status.InvalidArgumentErrorf("%s exceeds %d bytes", name, maxBytes)
	}
	if !utf8.ValidString(value) {
		return status.InvalidArgumentErrorf("%s is not valid UTF-8", name)
	}
	if strings.ContainsRune(value, '\x00') {
		return status.InvalidArgumentErrorf("%s contains NUL", name)
	}
	return nil
}
