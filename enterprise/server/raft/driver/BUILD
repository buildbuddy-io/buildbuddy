load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "driver",
    srcs = ["driver.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/driver",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/replica",
        "//enterprise/server/raft/storemap",
        "//proto:raft_go_proto",
        "//server/interfaces",
        "//server/util/alert",
        "//server/util/log",
        "@com_github_jonboulle_clockwork//:clockwork",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
