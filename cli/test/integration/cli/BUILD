load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

# Bazel version used to test overriding the .bazelversion bazel via
# BB_USE_BAZEL_VERSION. Must differ from the testbazel version.
OVERRIDE_BAZEL_VERSION = "8.5.0"

OVERRIDE_BAZEL = "//server/util/bazel:bazel-{}_crossplatform".format(OVERRIDE_BAZEL_VERSION)

go_test(
    name = "cli_test",
    srcs = ["cli_test.go"],
    data = [OVERRIDE_BAZEL],
    exec_properties = {
        # TODO: remove network dependency.
        "test.dockerNetwork": "bridge",
    },
    shard_count = 4,
    x_defs = {
        "overrideBazelRlocationpath": "$(rlocationpath {})".format(OVERRIDE_BAZEL),
        "overrideBazelVersion": OVERRIDE_BAZEL_VERSION,
    },
    deps = [
        "//cli/log",
        "//cli/parser",
        "//cli/parser/test_data",
        "//cli/testutil/testcli",
        "//proto:cache_go_proto",
        "//proto:invocation_go_proto",
        "//server/testutil/buildbuddy",
        "//server/testutil/testbazel",
        "//server/testutil/testfs",
        "//server/testutil/testgit",
        "//server/util/retry",
        "@com_github_google_uuid//:uuid",
        "@com_github_stretchr_testify//require",
    ],
)
