load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "filecache",
    srcs = ["filecache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/util/disk",
        "//server/util/fastcopy",
        "//server/util/log",
        "@com_github_google_uuid//:uuid",
    ],
)
