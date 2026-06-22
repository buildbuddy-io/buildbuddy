"""Bazel versions used by tests and probers."""

# Bazel binaries bundled into the prober image.
#
# When adding a cloudprober probe for a new Bazel version in buildbuddy-internal,
# add the version here first so the prober image contains /bazel/bazel-$VERSION.
BAZEL_PROBER_VERSIONS = [
    "6.5.0",
    "7.7.1",
    "8.3.1",
    "8.4.2",
    "8.5.0",
    "9.0.0rc3",
    "9.1.1",
]

# Bazel binary used by //server/testutil/testbazel.
TESTBAZEL_VERSION = "9.1.1"
