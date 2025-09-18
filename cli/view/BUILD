load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "view",
    srcs = ["view.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/view",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/login",
        "//proto:buildbuddy_service_go_proto",
        "//proto:eventlog_go_proto",
        "//server/util/grpc_client",
        "@org_golang_google_grpc//metadata",
    ],
)
