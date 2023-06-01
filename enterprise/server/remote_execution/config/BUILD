load("@io_bazel_rules_go//go:def.bzl", "go_library")

# TODO(sluongng): ensure all //enterprise code are not dependency of FOSS code
# gazelle:default_visibility //enterprise:__subpackages__,//server/buildbuddy_server:__pkg__,//server/static:__pkg__,//server/telemetry:__pkg__
package(default_visibility = [
    "//enterprise:__subpackages__",
    "//server/buildbuddy_server:__pkg__",
    "//server/static:__pkg__",
    "//server/telemetry:__pkg__",
])

go_library(
    name = "config",
    srcs = ["config.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/config",
)
