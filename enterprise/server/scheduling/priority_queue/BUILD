load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "priority_queue",
    srcs = ["priority_queue.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/priority_queue",
    deps = ["//proto:scheduler_go_proto"],
)
