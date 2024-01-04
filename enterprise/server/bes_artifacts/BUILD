load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "bes_artifacts",
    srcs = ["bes_artifacts.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/bes_artifacts",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/build_event_publisher",
        "//proto:build_event_stream_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/grpc_client",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_x_sync//errgroup",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
