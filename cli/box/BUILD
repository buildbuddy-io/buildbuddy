load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "box",
    srcs = ["box.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/box",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/login",
        "//cli/version",
        "//proto:buildbuddy_service_go_proto",
        "//proto:eventlog_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/real_environment",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/platform",
        "//server/util/rexec",
        "//server/util/uuid",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//types/known/durationpb",
    ],
)
