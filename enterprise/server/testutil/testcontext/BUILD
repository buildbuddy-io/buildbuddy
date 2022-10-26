load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "testcontext",
    srcs = ["test_context.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testcontext",
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/util/bazel_request",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//proto",
    ],
)
