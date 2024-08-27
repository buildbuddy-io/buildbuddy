load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "replay_invocation_actions_lib",
    srcs = ["replay_invocation_actions.go"],
    data = ["//enterprise/tools/replay_action"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/replay_invocation_actions",
    visibility = ["//visibility:private"],
    x_defs = {
        "replayActionRlocationpath": "$(rlocationpath //enterprise/tools/replay_action)",
    },
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//proto:buildbuddy_service_go_proto",
        "//proto:context_go_proto",
        "//proto:execution_stats_go_proto",
        "//proto:invocation_go_proto",
        "//proto:invocation_status_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/real_environment",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/rexec",
        "@io_bazel_rules_go//go/runfiles:go_default_library",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//types/known/durationpb",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "replay_invocation_actions",
    embed = [":replay_invocation_actions_lib"],
    pure = "on",
    static = "on",
    visibility = ["//visibility:public"],
)
