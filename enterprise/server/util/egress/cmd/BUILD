load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_binary(
    name = "cmd",
    embed = [":cmd_lib"],
)

go_library(
    name = "cmd_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/egress/cmd",
)
