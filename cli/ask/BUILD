load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "ask",
    srcs = ["ask.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/ask",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/login",
        "//cli/storage",
        "//cli/workspace",
        "//proto:buildbuddy_service_go_proto",
        "//proto:suggestion_go_proto",
        "//server/util/grpc_client",
        "//server/util/hash",
        "//server/util/uuid",
        "@org_golang_google_grpc//metadata",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])
