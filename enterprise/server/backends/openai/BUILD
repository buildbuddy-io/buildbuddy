load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "openai",
    srcs = ["openai.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/openai",
    deps = [
        "//server/util/log",
        "//server/util/status",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
