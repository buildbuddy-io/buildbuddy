load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "secrets",
    srcs = ["secrets.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/secrets",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/login",
        "//proto:buildbuddy_service_go_proto",
        "//proto:secrets_go_proto",
        "//server/util/grpc_client",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//metadata",
        "@org_golang_x_crypto//nacl/box",
    ],
)

go_test(
    name = "secrets_test",
    srcs = ["secrets_test.go"],
    embed = [":secrets"],
    deps = [
        "//proto:buildbuddy_service_go_proto",
        "//proto:secrets_go_proto",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_x_crypto//nacl/box",
    ],
)
