load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "cli_test",
    srcs = ["cli_test.go"],
    exec_properties = {
        # TODO: remove network dependency.
        "test.dockerNetwork": "bridge",
    },
    shard_count = 4,
    deps = [
        "//cli/log",
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

package(default_visibility = ["//cli:__subpackages__"])
