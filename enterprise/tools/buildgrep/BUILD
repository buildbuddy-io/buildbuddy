load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "buildgrep_lib",
    srcs = ["buildgrep.go"],
    data = ["//enterprise/tools/buildgrep/worker:buildgrep_worker_linux_amd64"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/buildgrep",
    visibility = ["//visibility:private"],
    x_defs = {"workerRlocationpath": "$(rlocationpath //enterprise/tools/buildgrep/worker:buildgrep_worker_linux_amd64)"},
    deps = [
        "//proto:buildbuddy_service_go_proto",
        "//proto:invocation_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/real_environment",
        "//server/remote_cache/cachetools",
        "//server/util/authutil",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/platform",
        "//server/util/rexec",
        "@io_bazel_rules_go//go/runfiles",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//types/known/durationpb",
        "@org_golang_google_protobuf//types/known/timestamppb",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "buildgrep",
    embed = [":buildgrep_lib"],
    visibility = ["//visibility:public"],
)
