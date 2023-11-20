load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "cgroup",
    srcs = ["cgroup.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/cgroup",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/util/log",
        "//server/util/status",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
