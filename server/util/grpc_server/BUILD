load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "grpc_server",
    srcs = ["grpc_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/grpc_server",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:buildbuddy_service_go_proto",
        "//proto:health_go_proto",
        "//proto:publish_build_event_go_proto",
        "//proto:remote_asset_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//proto/api/v1:api_v1_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/rpc/filters",
        "//server/util/log",
        "//server/util/status",
        "@com_github_grpc_ecosystem_go_grpc_prometheus//:go-grpc-prometheus",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//encoding/gzip",
        "@org_golang_google_grpc//keepalive",
        "@org_golang_google_grpc//reflection",
    ],
)
