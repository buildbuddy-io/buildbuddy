load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "rbesxs_lib",
    srcs = ["rbesxs.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/rbesxs",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//proto:remote_execution_go_proto",
        "//server/util/log",
        "//server/util/random",
    ],
)

go_binary(
    name = "rbesxs",
    embed = [":rbesxs_lib"],
)
