load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/ci_runner",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/workflowconf:go_default_library",
        "//proto:build_event_stream_go_proto",
        "//proto:build_events_go_proto",
        "//proto:publish_build_event_go_proto",
        "//server/util/grpc_client:go_default_library",
        "//server/util/lockingbuffer:go_default_library",
        "//server/util/status:go_default_library",
        "@com_github_golang_protobuf//ptypes:go_default_library_gen",
        "@com_github_google_shlex//:go_default_library",
        "@com_github_google_uuid//:go_default_library",
        "@com_github_logrusorgru_aurora//:go_default_library",
        "@in_gopkg_yaml_v2//:go_default_library",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//codes:go_default_library",
        "@org_golang_google_grpc//status:go_default_library",
    ],
)

go_binary(
    name = "ci_runner",
    embed = [":go_default_library"],
    visibility = ["//visibility:public"],
)
