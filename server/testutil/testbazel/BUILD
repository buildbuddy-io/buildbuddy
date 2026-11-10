load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")
load("//server/util/bazel:defs.bzl", "bazel_binary_targets")

# The default version used by the testbazel library.
DEFAULT_VERSION = "9.3.2"

# Other LTS versions that tests can run against.
LTS_VERSIONS = [
    "8.8.2",
    "7.7.2",
]

# Release candidates. These are tagged manual so that `bazel build //...`
# doesn't build (and download) them.
RC_VERSIONS = [
    "10.0.0rc2",
    "8.9.0rc1",
]

bazel_binary_targets(
    versions = [DEFAULT_VERSION] + LTS_VERSIONS,
    visibility = ["//visibility:public"],
)

bazel_binary_targets(
    tags = [
        "bazel-rc",
        "manual",
    ],
    versions = RC_VERSIONS,
    visibility = ["//visibility:public"],
)

go_library(
    name = "testbazel",
    testonly = 1,
    srcs = ["testbazel.go"],
    data = [
        ":bazel-%s_crossplatform" % DEFAULT_VERSION,
        ":bazel-%s_extract_installation" % DEFAULT_VERSION,
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel",
    visibility = ["//visibility:public"],
    x_defs = {
        "Version": DEFAULT_VERSION,
        "bazelRlocationpath": "$(rlocationpath :bazel-%s_crossplatform)" % DEFAULT_VERSION,
        "outdirRlocationpath": "$(rlocationpath :bazel-%s_extract_installation)" % DEFAULT_VERSION,
    },
    deps = [
        "//server/testutil/testfs",
        "//server/util/bazel",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)

_TEST_DEPS = [
    ":testbazel",
    "@com_github_stretchr_testify//require",
]

go_test(
    name = "testbazel_test",
    srcs = ["testbazel_test.go"],
    tags = ["block-network"],
    deps = [
        ":testbazel",
        "@com_github_stretchr_testify//require",
    ],
)

# Run the smoke test against each non-default version by overriding the
# testbazel library's x_defs.
[
    go_test(
        name = "testbazel_test_bazel_" + version.replace(".", "_"),
        srcs = ["testbazel_test.go"],
        data = [
            ":bazel-%s_crossplatform" % version,
            ":bazel-%s_extract_installation" % version,
        ],
        tags = ["block-network"] + ([
            "manual",
            "bazel-rc",
        ] if version in RC_VERSIONS else []),
        x_defs = {
            "github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel.Version": version,
            "github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel.bazelRlocationpath": "$(rlocationpath :bazel-%s_crossplatform)" % version,
            "github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel.outdirRlocationpath": "$(rlocationpath :bazel-%s_extract_installation)" % version,
        },
        deps = _TEST_DEPS,
    )
    for version in LTS_VERSIONS + RC_VERSIONS
]
