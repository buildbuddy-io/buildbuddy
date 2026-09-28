load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_test(
    name = "cli_test",
    srcs = ["cli_test.go"],
    data = ["//server/util/bazel:bazel-8.5.0_crossplatform"],
    exec_properties = {
        # TODO: remove network dependency.
        "test.dockerNetwork": "bridge",
    },
    shard_count = 4,
    x_defs = {
        "bazel8Rlocationpath": "$(rlocationpath //server/util/bazel:bazel-8.5.0_crossplatform)",
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
