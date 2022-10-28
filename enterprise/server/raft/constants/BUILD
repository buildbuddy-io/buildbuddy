load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "constants",
    srcs = ["constants.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/constants",
    deps = ["//enterprise/server/raft/keys"],
)
