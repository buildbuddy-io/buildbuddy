load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "cache_proxy_test",
    size = "small",
    timeout = "short",
    srcs = ["cache_proxy_test.go"],
    deps = [
        "//enterprise/server/test/integration/remote_execution/rbetest",
        "//proto:encryption_go_proto",
        "//proto:remote_execution_go_proto",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//metadata",
    ],
)
