load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "mocksaml_lib",
    srcs = ["mocksaml.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/mocksaml",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/util/mocksaml",
        "//server/util/log",
    ],
)

go_binary(
    name = "mocksaml",
    embed = [":mocksaml_lib"],
)
