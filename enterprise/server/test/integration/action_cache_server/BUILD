load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "action_cache_server_test",
    srcs = ["action_cache_server_test.go"],
    deps = [
        "//enterprise/server/clientidentity",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/testutil/testcache",
        "//server/testutil/testenv",
        "//server/util/random",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
