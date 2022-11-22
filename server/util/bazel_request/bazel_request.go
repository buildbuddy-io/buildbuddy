package bazel_request

import (
	"context"
	"regexp"
	"strconv"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	// Note: Using regexp instead of semver package for bazel version parsing,
	// since semver doesn't support things like "5.0.0rc1"
	bazelVersionPattern = regexp.MustCompile(`^(?P<major>\d+)\.(?P<minor>\d+)(\.(?P<patch>\d+))?(?P<suffix>.*)?`)
)

const RequestMetadataKey = "build.bazel.remote.execution.v2.requestmetadata-bin"

func GetRequestMetadata(ctx context.Context) *repb.RequestMetadata {
	if grpcMD, ok := metadata.FromIncomingContext(ctx); ok {
		rmdVals := grpcMD[RequestMetadataKey]
		for _, rmdVal := range rmdVals {
			rmd := &repb.RequestMetadata{}
			if err := proto.Unmarshal([]byte(rmdVal), rmd); err == nil {
				return rmd
			}
		}
	}
	return nil
}

func GetInvocationID(ctx context.Context) string {
	iid := ""
	if rmd := GetRequestMetadata(ctx); rmd != nil {
		iid = rmd.GetToolInvocationId()
	}
	return iid
}

type Version struct {
	// Major, Minor, Patch are the semver version parts. For Bazel versions like
	// "0.X", the Major and Patch versions will be 0, and the Minor version will
	// be "X".
	Major, Minor, Patch int

	// Suffix is the raw suffix occurring immediately after the version numbers.
	// Example: "-pre.20221102.3"
	Suffix string
}

// ParseVersion attempts to parse a Bazel version from a string.
func ParseVersion(spec string) (*Version, error) {
	m := bazelVersionPattern.FindStringSubmatch(spec)
	if len(m) == 0 {
		return nil, status.InvalidArgumentErrorf("invalid tool version %q", spec)
	}
	var err error
	v := &Version{}
	v.Major, err = strconv.Atoi(m[bazelVersionPattern.SubexpIndex("major")])
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid major version: %s", err)
	}
	v.Minor, err = strconv.Atoi(m[bazelVersionPattern.SubexpIndex("minor")])
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid minor version: %s", err)
	}
	if p := m[bazelVersionPattern.SubexpIndex("patch")]; p != "" {
		v.Patch, err = strconv.Atoi(p)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("invalid patch version: %s", err)
		}
	}
	v.Suffix = m[bazelVersionPattern.SubexpIndex("suffix")]
	return v, nil
}

// GetVersion returns the parsed Bazel version from the context. It returns nil
// if no Bazel version could be parsed, and in particular if the client is not
// bazel.
func GetVersion(ctx context.Context) *Version {
	rmd := GetRequestMetadata(ctx)
	if rmd == nil {
		return nil
	}
	if rmd.GetToolDetails().GetToolName() != "bazel" {
		return nil
	}
	v, _ := ParseVersion(rmd.GetToolDetails().GetToolVersion())
	return v
}

// IsAtLeast returns whether this bazel version is equal to or supercedes the
// given version.
func (v *Version) IsAtLeast(c *Version) bool {
	if v.Major != c.Major {
		return v.Major > c.Major
	}
	if v.Minor != c.Minor {
		return v.Minor > c.Minor
	}
	if v.Patch != c.Patch {
		return v.Patch > c.Patch
	}
	// If major, minor, and patch versions are all the same, compare the suffix.
	// Make sure that release versions (which have an empty suffix) are
	// considered greater than pre-release versions (which have a non-empty
	// suffix).
	vReleaseBit := boolToInt(v.Suffix == "")
	cReleaseBit := boolToInt(c.Suffix == "")
	if vReleaseBit != cReleaseBit {
		return vReleaseBit > cReleaseBit
	}
	// If neither is a release version, compare pre-release suffixes
	// lexicographically.
	if v.Suffix != c.Suffix {
		return v.Suffix > c.Suffix
	}
	// v is exactly equal to c, so it is at least c.
	return true
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func WithRequestMetadata(ctx context.Context, md *repb.RequestMetadata) (context.Context, error) {
	if rmd := GetRequestMetadata(ctx); rmd != nil {
		return nil, status.FailedPreconditionError("context already has request metadata")
	}
	mdBytes, err := proto.Marshal(md)
	if err != nil {
		return nil, err
	}
	return metadata.AppendToOutgoingContext(ctx, RequestMetadataKey, string(mdBytes)), nil
}
