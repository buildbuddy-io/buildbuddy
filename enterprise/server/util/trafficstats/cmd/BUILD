load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_binary(
    name = "cmd",
    embed = [":cmd_lib"],
)

go_library(
    name = "cmd_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/trafficstats/cmd",
    deps = ["//enterprise/server/util/trafficstats/providerips"],
)

go_test(
    name = "cmd_test",
    srcs = ["main_test.go"],
    embed = [":cmd_lib"],
    deps = ["//enterprise/server/util/trafficstats/providerips"],
)
