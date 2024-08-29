load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "tee_analyze_lib",
    srcs = ["tee_analyze.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/tee_analyze",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:api_key_go_proto",
        "//proto:buildbuddy_service_go_proto",
        "//proto:context_go_proto",
        "//proto:group_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/real_environment",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/rexec",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//status",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "tee_analyze",
    embed = [":tee_analyze_lib"],
    visibility = ["//visibility:public"],
)

package(default_visibility = ["//enterprise:__subpackages__"])
