load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "login",
    srcs = ["login.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/login",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/parser",
        "//cli/storage",
        "//proto:buildbuddy_service_go_proto",
        "//proto:user_go_proto",
        "//server/util/grpc_client",
        "//server/util/status",
        "@org_golang_google_grpc//metadata",
    ],
)
