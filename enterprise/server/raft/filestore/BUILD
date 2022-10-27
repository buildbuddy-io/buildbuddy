load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "filestore",
    srcs = ["filestore.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore",
    deps = [
        "//proto:raft_go_proto",
        "//proto:resource_go_proto",
        "//server/interfaces",
        "//server/util/disk",
        "//server/util/log",
        "//server/util/status",
    ],
)
