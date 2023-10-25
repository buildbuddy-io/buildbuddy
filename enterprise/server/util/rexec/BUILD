load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "rexec",
    srcs = ["rexec.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/rexec",
    deps = [
        "//enterprise/server/remote_execution/operation",
        "//enterprise/server/remote_execution/platform",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "@org_golang_google_protobuf//types/known/durationpb",
        "@org_golang_x_sync//errgroup",
    ],
)
