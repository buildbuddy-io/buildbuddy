load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "run_remote_lib",
    srcs = ["run_remote.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/run_remote",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/real_environment",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/retry",
        "//server/util/status",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//status",
    ],
)

go_binary(
    name = "run_remote",
    embed = [":run_remote_lib"],
    pure = "on",
    static = "on",
)

package(default_visibility = ["//enterprise:__subpackages__"])
