load("@io_bazel_rules_go//go:def.bzl", "go_binary")

package(default_visibility = ["//enterprise:__subpackages__"])

go_binary(
    name = "simulator",
    srcs = ["simulator.go"],
    deps = ["//enterprise/server/raft/driver"],
)
