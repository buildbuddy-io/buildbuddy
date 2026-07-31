// Package identity validates TestBuddy target and case addresses.
package identity

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
)

const (
	MaxAddressBytes = 1024

	MaxRepositoryURLBytes = 512
	MaxTargetLabelBytes   = 512
	MaxPackagePathBytes   = 512
	MaxCaseNameBytes      = 512
)

type TargetAddress struct {
	Repository  string
	TargetLabel string
}

type CaseAddress struct {
	Repository  string
	TargetLabel string
	CaseName    string
}

func (a CaseAddress) Target() TargetAddress {
	return TargetAddress{Repository: a.Repository, TargetLabel: a.TargetLabel}
}

func (a CaseAddress) String() string {
	return fmt.Sprintf("%s:%s:%s", strconv.Quote(a.Repository),
		strconv.Quote(a.TargetLabel), strconv.Quote(a.CaseName))
}

func (a TargetAddress) Subject() CaseAddress {
	return CaseAddress{Repository: a.Repository, TargetLabel: a.TargetLabel}
}

func (a TargetAddress) String() string {
	return fmt.Sprintf("%s:%s", strconv.Quote(a.Repository),
		strconv.Quote(a.TargetLabel))
}

func (a TargetAddress) IsZero() bool { return a == TargetAddress{} }

func (a CaseAddress) IsZero() bool { return a == CaseAddress{} }

type CaseInput struct {
	RepositoryURL string
	TargetLabel   string
	CaseName      string
}

type Target struct {
	Label               string
	RepositoryQualifier string
	PackagePath         string
	Name                string
}

type Identity struct {
	Address CaseAddress
	Target  Target
}

type TargetIdentity struct {
	Address TargetAddress
	Target  Target
}

func CanonicalizeCase(in CaseInput) (*Identity, error) {
	repository, err := NormalizeRepositoryURL(in.RepositoryURL)
	if err != nil {
		return nil, err
	}
	target, err := CanonicalizeTarget(in.TargetLabel)
	if err != nil {
		return nil, err
	}
	if in.CaseName == "" {
		return nil, status.InvalidArgumentError("case name is required")
	}
	if err := ValidateAddressComponent("case name", in.CaseName, MaxCaseNameBytes); err != nil {
		return nil, err
	}

	address := CaseAddress{
		Repository:  repository,
		TargetLabel: target.Label,
		CaseName:    in.CaseName,
	}
	if err := checkAddressLength(address.String()); err != nil {
		return nil, err
	}
	return &Identity{Address: address, Target: target}, nil
}

func CanonicalizeTargetIdentity(repositoryURL, targetLabel string) (*TargetIdentity, error) {
	repository, err := NormalizeRepositoryURL(repositoryURL)
	if err != nil {
		return nil, err
	}
	target, err := CanonicalizeTarget(targetLabel)
	if err != nil {
		return nil, err
	}
	address := TargetAddress{Repository: repository, TargetLabel: target.Label}
	if err := checkAddressLength(address.String()); err != nil {
		return nil, err
	}
	return &TargetIdentity{Address: address, Target: target}, nil
}

func checkAddressLength(rendered string) error {
	if len(rendered) > MaxAddressBytes {
		return status.InvalidArgumentErrorf(
			"test address is %d bytes, over the %d-byte limit, and is rejected rather than shortened: %s",
			len(rendered), MaxAddressBytes, rendered)
	}
	return nil
}

func ValidateCaseAddress(address CaseAddress) error {
	if err := ValidateTargetAddress(address.Target()); err != nil {
		return err
	}
	if address.CaseName == "" {
		return status.InvalidArgumentError("case name is required")
	}
	if err := ValidateAddressComponent("case name", address.CaseName, MaxCaseNameBytes); err != nil {
		return err
	}
	return checkAddressLength(address.String())
}

func ValidateTargetAddress(address TargetAddress) error {
	if address.Repository == "" {
		return status.InvalidArgumentError("repository URL is required")
	}
	if err := ValidateAddressComponent("repository URL", address.Repository, MaxRepositoryURLBytes); err != nil {
		return err
	}
	if _, err := CanonicalizeTarget(address.TargetLabel); err != nil {
		return err
	}
	return checkAddressLength(address.String())
}

func (id *Identity) Proto() *tbpb.TestCaseIdentity { return CaseProto(id.Address) }

func (t *TargetIdentity) Proto() *tbpb.TestTargetIdentity { return TargetProto(t.Address) }

func CaseProto(address CaseAddress) *tbpb.TestCaseIdentity {
	return &tbpb.TestCaseIdentity{
		Target:   TargetProto(address.Target()),
		CaseName: address.CaseName,
	}
}

func TargetProto(address TargetAddress) *tbpb.TestTargetIdentity {
	return &tbpb.TestTargetIdentity{
		RepoUrl:     address.Repository,
		TargetLabel: address.TargetLabel,
	}
}

func CaseAddressFromProto(in *tbpb.TestCaseIdentity) CaseAddress {
	return CaseAddress{
		Repository:  in.GetTarget().GetRepoUrl(),
		TargetLabel: in.GetTarget().GetTargetLabel(),
		CaseName:    in.GetCaseName(),
	}
}

// NormalizeRepositoryURL normalizes a repository URL to the canonical
// portable form used by addresses. Batch callers can validate the repository
// once instead of rejecting every record in a report individually.
func NormalizeRepositoryURL(raw string) (string, error) {
	// git.NormalizeRepoURL reads an uppercase scheme as part of the host.
	if scheme, rest, ok := strings.Cut(raw, "://"); ok && !strings.ContainsAny(scheme, "/@") {
		raw = strings.ToLower(scheme) + "://" + rest
	}
	normalized, err := gitutil.NormalizeRepoURL(raw)
	if err != nil {
		return "", status.InvalidArgumentErrorf("normalize repository URL: %s", err)
	}
	if normalized.String() == "" {
		return "", status.InvalidArgumentError("repository URL is required")
	}
	normalized.Scheme = strings.ToLower(normalized.Scheme)
	normalized.Host = strings.ToLower(normalized.Host)
	normalized.RawQuery = ""
	normalized.ForceQuery = false
	normalized.Fragment = ""
	normalized.RawFragment = ""
	if normalized.Path != "/" {
		normalized.Path = strings.TrimSuffix(normalized.Path, "/")
		normalized.Path = strings.TrimSuffix(normalized.Path, ".git")
	}
	value := normalized.String()
	if err := ValidateAddressComponent("repository URL", value, MaxRepositoryURLBytes); err != nil {
		return "", err
	}
	return value, nil
}

// CanonicalizeTarget validates an absolute Bazel target label and
// canonicalizes shorthand ("//pkg" -> "//pkg:pkg", "@//x" -> "//x"),
// preserving external repository qualifiers exactly.
func CanonicalizeTarget(raw string) (Target, error) {
	if err := ValidateAddressComponent("target label", raw, MaxTargetLabelBytes); err != nil {
		return Target{}, err
	}
	if raw == "" {
		return Target{}, status.InvalidArgumentError("target label is required")
	}
	repositoryQualifier := ""
	remainder := raw
	switch {
	case strings.HasPrefix(raw, "@//"), strings.HasPrefix(raw, "@@//"):
		// Within a report, "//" is relative to the reported repository, so
		// both main-repository spellings ("@//" apparent, "@@//" canonical)
		// normalize to "//".
		remainder = raw[strings.Index(raw, "//"):]
	case strings.HasPrefix(raw, "@"):
		separator := strings.Index(raw, "//")
		if separator < 2 {
			return Target{}, status.InvalidArgumentErrorf("external target label %q is missing //", raw)
		}
		repositoryQualifier = raw[:separator]
		repositoryName := strings.TrimLeft(repositoryQualifier, "@")
		if !validRepositoryName(repositoryName) {
			return Target{}, status.InvalidArgumentErrorf("external repository qualifier %q is invalid", repositoryQualifier)
		}
		remainder = raw[separator:]
	case !strings.HasPrefix(raw, "//"):
		return Target{}, status.InvalidArgumentErrorf("target label %q must be absolute", raw)
	}

	remainder = strings.TrimPrefix(remainder, "//")
	if strings.Count(remainder, ":") > 1 {
		return Target{}, status.InvalidArgumentErrorf("target label %q contains multiple colons", raw)
	}
	packagePath, targetName, hasColon := strings.Cut(remainder, ":")
	if !hasColon {
		if packagePath == "" {
			return Target{}, status.InvalidArgumentErrorf("root-package target label %q must name a target", raw)
		}
		targetName = packagePath[strings.LastIndex(packagePath, "/")+1:]
	}
	if targetName == "" {
		return Target{}, status.InvalidArgumentErrorf("target label %q has an empty target name", raw)
	}
	if err := validatePackagePath(packagePath); err != nil {
		return Target{}, status.InvalidArgumentErrorf("target label %q has an invalid package path: %s", raw, err)
	}
	if err := validateTargetName(targetName); err != nil {
		return Target{}, status.InvalidArgumentErrorf("target label %q has an invalid target name: %s", raw, err)
	}

	label := repositoryQualifier + "//" + packagePath + ":" + targetName
	if err := ValidateAddressComponent("target label", label, MaxTargetLabelBytes); err != nil {
		return Target{}, err
	}
	return Target{
		Label:               label,
		RepositoryQualifier: repositoryQualifier,
		PackagePath:         packagePath,
		Name:                targetName,
	}, nil
}

// validRepositoryName accepts the characters Bazel allows in an apparent or
// canonical external repository name. Canonical names are mangled from module
// name and version, which is where "+" (Bazel 8 and later) and "~" (Bazel 7)
// come from; a report is rejected outright if this is too strict, so it stays
// deliberately permissive.
func validRepositoryName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if !(r >= 'a' && r <= 'z') &&
			!(r >= 'A' && r <= 'Z') &&
			!(r >= '0' && r <= '9') &&
			r != '-' && r != '_' && r != '.' && r != '+' && r != '~' {
			return false
		}
	}
	return true
}

func validatePackagePath(packagePath string) error {
	if packagePath == "" {
		return nil
	}
	if len(packagePath) > MaxPackagePathBytes {
		return status.InvalidArgumentErrorf("exceeds %d bytes", MaxPackagePathBytes)
	}
	for _, r := range packagePath {
		if r > 127 || r <= 31 || r == 127 || r == ':' || r == '\\' {
			return status.InvalidArgumentErrorf("invalid character %q", r)
		}
	}
	for segment := range strings.SplitSeq(packagePath, "/") {
		if segment == "" || strings.Trim(segment, ".") == "" {
			return status.InvalidArgumentError("empty or dot-only path segment")
		}
	}
	return nil
}

func validateTargetName(targetName string) error {
	if strings.HasPrefix(targetName, "/") || strings.HasSuffix(targetName, "/") {
		return status.InvalidArgumentError("target name must not begin or end with /")
	}
	if strings.Contains(targetName, "//") {
		return status.InvalidArgumentError("target name must not contain //")
	}
	for segment := range strings.SplitSeq(targetName, "/") {
		if segment == "." || segment == ".." {
			return status.InvalidArgumentError("target name contains a relative path segment")
		}
	}
	for _, r := range targetName {
		if r <= 31 || r == 127 || r == ':' || r == '\\' {
			return status.InvalidArgumentErrorf("invalid character %q", r)
		}
	}
	return nil
}

// ValidateAddressComponent validates one stored address field.
func ValidateAddressComponent(name, value string, maxBytes int) error {
	return ValidatePrintableASCII(name, value, maxBytes)
}

func ValidatePrintableASCII(name, value string, maxBytes int) error {
	if err := ValidateBoundedString(name, value, maxBytes); err != nil {
		return err
	}
	for i := 0; i < len(value); i++ {
		if c := value[i]; c < 0x20 || c > 0x7e {
			return status.InvalidArgumentErrorf(
				"%s must be printable ASCII for TestBuddy storage; byte %d is %#02x", name, i, c)
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
