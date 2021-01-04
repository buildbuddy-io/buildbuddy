load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["distributed_disk.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/distributed_disk",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/backends/pubsub:go_default_library",
        "//enterprise/server/util/diskproxy:go_default_library",
        "//proto:remote_execution_go_proto",
        "//server/interfaces:go_default_library",
        "//server/remote_cache/digest:go_default_library",
        "//server/util/consistent_hash:go_default_library",
        "//server/util/disk:go_default_library",
        "//server/util/prefix:go_default_library",
        "//server/util/status:go_default_library",
    ],
)
