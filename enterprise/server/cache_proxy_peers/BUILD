load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "cache_proxy_peers",
    srcs = ["cache_proxy_peers.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cache_proxy_peers",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/remote_cache/digest",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/rangemap",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
    ],
)

go_test(
    name = "cache_proxy_peers_test",
    size = "small",
    srcs = ["cache_proxy_peers_test.go"],
    deps = [
        ":cache_proxy_peers",
        "//server/remote_cache/digest",
        "//server/testutil/testenv",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)
