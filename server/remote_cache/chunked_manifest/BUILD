load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "chunking",
    srcs = ["chunking.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/chunking",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
    ],
)

go_library(
    name = "chunked_manifest",
    srcs = ["chunking.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/chunked_manifest",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
    ],
)
