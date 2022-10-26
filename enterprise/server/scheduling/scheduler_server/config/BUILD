load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = [
    "//enterprise:__subpackages__",
    "//server/static:__pkg__",
])

go_library(
    name = "config",
    srcs = ["config.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/scheduling/scheduler_server/config",
)
