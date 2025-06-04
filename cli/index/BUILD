load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "index",
    srcs = ["index.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/index",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/login",
        "//cli/storage",
        "//codesearch/github",
        "//proto:buildbuddy_service_go_proto",
        "//proto:git_go_proto",
        "//proto:index_go_proto",
        "//server/util/git",
        "//server/util/grpc_client",
        "//server/util/status",
        "@org_golang_google_grpc//metadata",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])
