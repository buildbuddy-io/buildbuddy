load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "real_environment",
    srcs = ["real_environment.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/real_environment",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:publish_build_event_go_proto",
        "//proto:remote_asset_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//proto:soci_go_proto",
        "//server/interfaces",
        "@com_github_go_redis_redis_v8//:redis",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//test/bufconn",
    ],
)
