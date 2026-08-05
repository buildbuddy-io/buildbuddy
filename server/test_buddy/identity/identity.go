// Package identity validates TestBuddy test addresses.
package identity

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/bazel-contrib/bazel-gazelle/v2/label"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
)

const (
	maxRepositoryURLBytes = 512
	maxPackagePathBytes   = 1024
	maxTargetNameBytes    = 512
	maxTargetLabelBytes   = maxPackagePathBytes + maxTargetNameBytes + len("//:")
	maxCaseNameBytes      = 512
)

type Address struct {
	Repository  string
	PackagePath string
	TargetName  string
	CaseName    string
}

func Canonicalize(repositoryURL string, test *tbpb.TestIdentity) (Address, error) {
	repository, err := NormalizeRepositoryURL(repositoryURL)
	if err != nil {
		return Address{}, err
	}
	packagePath, targetName, err := parseTargetLabel(test.GetTargetLabel())
	if err != nil {
		return Address{}, err
	}
	if err := ValidateCaseName(test.GetCaseName()); err != nil {
		return Address{}, err
	}
	return Address{
		Repository:  repository,
		PackagePath: packagePath,
		TargetName:  targetName,
		CaseName:    test.GetCaseName(),
	}, nil
}

func (a Address) Validate() error {
	canonical, err := Canonicalize(a.Repository, a.Proto())
	if err != nil {
		return err
	}
	if canonical != a {
		return status.InvalidArgumentError("test address is not canonical")
	}
	return nil
}

func (a Address) TargetLabel() string {
	return "//" + a.PackagePath + ":" + a.TargetName
}

func (a Address) Proto() *tbpb.TestIdentity {
	return &tbpb.TestIdentity{TargetLabel: a.TargetLabel(), CaseName: a.CaseName}
}

func (a Address) String() string {
	address := fmt.Sprintf("%s:%s:%s", strconv.Quote(a.Repository),
		strconv.Quote(a.PackagePath), strconv.Quote(a.TargetName))
	if a.CaseName != "" {
		address += ":" + strconv.Quote(a.CaseName)
	}
	return address
}

func NormalizeRepositoryURL(raw string) (string, error) {
	normalized, err := git.NormalizeRepoURL(raw)
	if err != nil {
		return "", status.InvalidArgumentErrorf("normalize repository URL: %s", err)
	}
	for {
		value := normalized.String()
		next, err := git.NormalizeRepoURL(value)
		if err != nil {
			return "", status.InvalidArgumentErrorf("normalize repository URL: %s", err)
		}
		if next.String() == value {
			break
		}
		normalized = next
	}
	value := normalized.String()
	if err := validatePrintableASCII("repository URL", value, maxRepositoryURLBytes); err != nil {
		return "", err
	}
	if normalized.Host == "" || strings.Trim(normalized.Path, "/") == "" {
		return "", status.InvalidArgumentError("repository URL must have a host and path")
	}
	if normalized.Scheme != "https" && !(normalized.Scheme == "http" && normalized.Hostname() == "localhost") {
		return "", status.InvalidArgumentErrorf("repository URL has unsupported scheme %q", normalized.Scheme)
	}
	if normalized.RawQuery != "" || normalized.Fragment != "" {
		return "", status.InvalidArgumentError("repository URL must not contain a query or fragment")
	}
	return value, nil
}

func CanonicalizeTargetLabel(raw string) (string, error) {
	packagePath, targetName, err := parseTargetLabel(raw)
	if err != nil {
		return "", err
	}
	return "//" + packagePath + ":" + targetName, nil
}

func parseTargetLabel(raw string) (string, string, error) {
	if raw == "" {
		return "", "", status.InvalidArgumentError("target label is required")
	}
	if err := validatePrintableASCII("target label", raw, maxTargetLabelBytes); err != nil {
		return "", "", err
	}
	parsed, err := label.Parse(raw)
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
	if parsed.Name == "all" || parsed.Name == "all-targets" || parsed.Name == "*" || parsed.Name == "..." {
		return "", "", status.InvalidArgumentErrorf("target label %q is a target pattern", raw)
	}
	if slices.Contains(strings.Split(parsed.Pkg, "/"), "...") {
		return "", "", status.InvalidArgumentErrorf("target label %q is a target pattern", raw)
	}
	if len(parsed.Pkg) > maxPackagePathBytes {
		return "", "", status.InvalidArgumentErrorf(
			"package path exceeds %d bytes", maxPackagePathBytes)
	}
	if len(parsed.Name) > maxTargetNameBytes {
		return "", "", status.InvalidArgumentErrorf(
			"target name exceeds %d bytes", maxTargetNameBytes)
	}
	return parsed.Pkg, parsed.Name, nil
}

func ValidateCaseName(caseName string) error {
	if caseName == "" {
		return nil
	}
	if err := validateBoundedString("case name", caseName, maxCaseNameBytes); err != nil {
		return err
	}
	for _, r := range caseName {
		if unicode.IsControl(r) {
			return status.InvalidArgumentErrorf("case name contains control character %U", r)
		}
	}
	return nil
}

func validatePrintableASCII(name, value string, maxBytes int) error {
	if err := validateBoundedString(name, value, maxBytes); err != nil {
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

func validateBoundedString(name, value string, maxBytes int) error {
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
