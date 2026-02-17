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
        "//server/testutil/testkeys",
        "//server/util/claims",
        "//server/util/grpc_server",
        "//server/util/testing/flags",
        "@com_github_golang_jwt_jwt_v4//:jwt",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//metadata",
    ],
)
