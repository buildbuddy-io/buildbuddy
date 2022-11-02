load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "container",
    srcs = ["container.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/container",
    deps = [
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/containers/docker",
        "//enterprise/server/util/ext4",
        "//server/util/disk",
        "//server/util/log",
        "//server/util/status",
        "@com_github_docker_docker//client",
    ],
)
