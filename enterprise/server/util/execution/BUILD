load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "execution",
    srcs = ["execution.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/execution",
    visibility = ["//enterprise:__subpackages__"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:stored_invocation_go_proto",
        "//server/tables",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
