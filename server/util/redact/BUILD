load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "redact",
    srcs = ["redact.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/redact",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//server/environment",
        "@com_github_golang_protobuf//proto:go_default_library",
    ],
)
