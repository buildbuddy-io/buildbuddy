load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "flake",
    srcs = ["flake.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/flake",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/login",
        "//cli/storage",
        "//proto:buildbuddy_service_go_proto",
        "//proto:target_go_proto",
        "//server/util/grpc_client",
        "@org_golang_google_grpc//metadata",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])
