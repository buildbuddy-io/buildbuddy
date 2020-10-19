load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["buildbuddy_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:bazel_config_go_proto",
        "//proto:context_go_proto",
        "//proto:execution_stats_go_proto",
        "//proto:group_go_proto",
        "//proto:invocation_go_proto",
        "//proto:user_go_proto",
        "//server/build_event_protocol/build_event_handler:go_default_library",
        "//server/bytestream:go_default_library",
        "//server/environment:go_default_library",
        "//server/ssl:go_default_library",
        "//server/tables:go_default_library",
        "//server/util/perms:go_default_library",
        "//server/util/request_context:go_default_library",
        "//server/util/status:go_default_library",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@org_golang_google_grpc//codes:go_default_library",
        "@org_golang_google_grpc//status:go_default_library",
    ],
)
