load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "execute",
    srcs = ["execute.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/execute",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//proto:remote_execution_go_proto",
        "//server/real_environment",
        "//server/remote_cache/digest",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/mdutil",
        "//server/util/rexec",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//types/known/durationpb",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])
