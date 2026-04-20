load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "config",
    srcs = ["config.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/config",
    deps = [
        "//enterprise/server/filestore",
        "//server/interfaces",
        "//server/util/alert",
        "//server/util/disk",
        "@com_github_lni_dragonboat_v4//config",
    ],
)
