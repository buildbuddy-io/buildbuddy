load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "distributed_cache_test",
    srcs = ["distributed_cache_test.go"],
    deps = [
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/testutil/testfs",
        "//server/testutil/testport",
        "//server/util/log",
        "//server/util/status",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_x_sync//errgroup",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
