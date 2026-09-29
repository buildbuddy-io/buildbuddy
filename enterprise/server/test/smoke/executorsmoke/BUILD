load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "executorsmoke",
    testonly = 1,
    srcs = [
        "binary.go",
        "executor.go",
        "executorsmoke.go",
        "tool.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/test/smoke/executorsmoke",
    deps = [
        "//proto:buildbuddy_service_go_proto",
        "//proto:context_go_proto",
        "//proto:execution_stats_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//server/real_environment",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/grpc_client",
        "//server/util/rexec",
        "//server/util/uuid",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//types/known/durationpb",
        "@org_golang_x_sync//errgroup",
    ],
)
