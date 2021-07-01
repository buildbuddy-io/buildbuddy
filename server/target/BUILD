load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "target",
    srcs = ["target.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/target",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:target_go_proto",
        "//proto/api/v1:common_go_proto",
        "//server/environment",
        "//server/util/db",
        "//server/util/perms",
        "//server/util/query_builder",
        "//server/util/status",
        "//server/util/timeutil",
        "@com_github_golang_protobuf//ptypes:go_default_library_gen",
    ],
)
