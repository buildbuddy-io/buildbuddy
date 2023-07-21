load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "chunker",
    srcs = ["chunker.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/chunker",
    deps = [
        "//enterprise/server/raft/filestore",
        "//proto:raft_go_proto",
        "//proto:resource_go_proto",
        "//server/util/background",
        "//server/util/status",
        "@com_github_jotfs_fastcdc_go//:fastcdc-go",
        "@org_golang_x_sync//errgroup",
    ],
)
