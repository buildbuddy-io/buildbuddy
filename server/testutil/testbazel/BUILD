load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")
load(
    "//server/util/bazel:defs.bzl",
    "TESTBAZEL_DEFAULT_VERSION",
    "TESTBAZEL_LTS_VERSIONS",
    "TESTBAZEL_RC_VERSIONS",
    "bazel_binary_targets",
    "bazel_versions_test",
)

bazel_binary_targets(
    versions = [TESTBAZEL_DEFAULT_VERSION] + TESTBAZEL_LTS_VERSIONS,
    visibility = ["//visibility:public"],
)

bazel_binary_targets(
    tags = [
        "bazel-rc",
        "manual",
    ],
    versions = TESTBAZEL_RC_VERSIONS,
    visibility = ["//visibility:public"],
)

go_library(
    name = "testbazel",
    testonly = 1,
    srcs = ["testbazel.go"],
    data = [
        ":bazel-%s_crossplatform" % TESTBAZEL_DEFAULT_VERSION,
        ":bazel-%s_extract_installation" % TESTBAZEL_DEFAULT_VERSION,
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel",
    visibility = ["//visibility:public"],
    x_defs = {
        "Version": TESTBAZEL_DEFAULT_VERSION,
        "bazelRlocationpath": "$(rlocationpath :bazel-%s_crossplatform)" % TESTBAZEL_DEFAULT_VERSION,
        "outdirRlocationpath": "$(rlocationpath :bazel-%s_extract_installation)" % TESTBAZEL_DEFAULT_VERSION,
    },
    deps = [
        "//server/testutil/testfs",
        "//server/util/bazel",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)

bazel_versions_test(
    name = "testbazel_test",
    srcs = ["testbazel_test.go"],
    rule = go_test,
    tags = ["block-network"],
    deps = [
        ":testbazel",
        "@com_github_stretchr_testify//require",
    ],
)
