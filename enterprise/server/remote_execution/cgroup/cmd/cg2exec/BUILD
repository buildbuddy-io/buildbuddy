load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "cg2exec_lib",
    srcs = ["cg2exec.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/cgroup/cmd/cg2exec",
)

go_binary(
    name = "cg2exec",
    embed = [":cg2exec_lib"],
)
