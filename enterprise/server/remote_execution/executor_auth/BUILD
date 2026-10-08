load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "executor_auth",
    srcs = ["executor_auth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/executor_auth",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/authutil",
        "//server/util/flag",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//metadata",
    ],
)

go_test(
    name = "executor_auth_test",
    size = "small",
    srcs = ["executor_auth_test.go"],
    deps = [
        ":executor_auth",
        "//server/testutil/testenv",
        "//server/util/authutil",
        "//server/util/grpc_client",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//health/grpc_health_v1",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//test/bufconn",
    ],
)
