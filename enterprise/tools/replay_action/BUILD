load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "replay_action_lib",
    srcs = ["replay_action.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy-internal/tools/replay_action",
    visibility = ["//visibility:private"],
    deps = [
        "@com_github_buildbuddy_io_buildbuddy//proto:remote_execution_go_proto",
        "@com_github_buildbuddy_io_buildbuddy//proto:resource_go_proto",
        "@com_github_buildbuddy_io_buildbuddy//server/remote_cache/cachetools",
        "@com_github_buildbuddy_io_buildbuddy//server/remote_cache/digest",
        "@com_github_buildbuddy_io_buildbuddy//server/util/flagutil",
        "@com_github_buildbuddy_io_buildbuddy//server/util/grpc_client",
        "@com_github_buildbuddy_io_buildbuddy//server/util/status",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@com_github_golang_protobuf//ptypes:go_default_library_gen",
        "@com_github_golang_protobuf//ptypes/timestamp",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "replay_action",
    embed = [":replay_action_lib"],
    visibility = ["//visibility:public"],
)
