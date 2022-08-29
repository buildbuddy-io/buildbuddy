load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "eventlog",
    srcs = ["eventlog.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/eventlog",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:eventlog_go_proto",
        "//proto:invocation_go_proto",
        "//server/backends/chunkstore",
        "//server/environment",
        "//server/interfaces",
        "//server/util/keyval",
        "//server/util/status",
        "@com_github_buildbuddy_io_buildbuddy//server/terminal:go_default_library",
        "@org_golang_google_protobuf//proto",
    ],
)
