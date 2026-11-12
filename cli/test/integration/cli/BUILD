load("@io_bazel_rules_go//go:def.bzl", "go_test")
load("//server/util/bazel:defs.bzl", "bazel_versions_test")

package(default_visibility = ["//cli:__subpackages__"])

bazel_versions_test(
    name = "cli_test",
    srcs = ["cli_test.go"],
    rule = go_test,
    shard_count = 4,
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
