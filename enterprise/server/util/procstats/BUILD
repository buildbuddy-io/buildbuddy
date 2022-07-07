load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "procstats",
    srcs = ["procstats.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/procstats",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/container",
        "@com_github_mitchellh_go_ps//:go-ps",
        "@com_github_shirou_gopsutil_v3//process",
    ],
)
