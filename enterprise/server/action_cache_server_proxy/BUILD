load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "action_cache_server_proxy",
    srcs = ["action_cache_server_proxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/action_cache_server_proxy",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/status",
    ],
)

go_test(
    name = "action_cache_server_proxy_test",
    size = "small",
    srcs = ["action_cache_server_proxy_test.go"],
    embed = [":action_cache_server_proxy"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/action_cache_server",
        "//server/testutil/testenv",
        "//server/util/status",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//:go_default_library",
    ],
)
