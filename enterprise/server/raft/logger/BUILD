load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "logger",
    srcs = ["logger.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/logger",
    deps = [
        "//server/util/log",
        "@com_github_lni_dragonboat_v3//logger",
    ],
)
