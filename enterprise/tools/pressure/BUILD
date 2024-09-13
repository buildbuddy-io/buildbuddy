load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "pressure_lib",
    srcs = ["pressure.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/pressure",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/real_environment",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/rexec",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//types/known/durationpb",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "pressure",
    embed = [":pressure_lib"],
    visibility = ["//visibility:public"],
)
