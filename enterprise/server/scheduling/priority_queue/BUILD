load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "priority_queue",
    srcs = ["priority_queue.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/priority_queue",
    deps = ["//proto:scheduler_go_proto"],
)

go_test(
    name = "priority_queue_test",
    srcs = ["priority_queue_test.go"],
    deps = [
        ":priority_queue",
        "//proto:scheduler_go_proto",
        "@com_github_stretchr_testify//require",
    ],
)
