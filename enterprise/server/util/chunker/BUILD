load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "chunker",
    srcs = ["chunker.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/chunker",
    deps = ["@com_github_jotfs_fastcdc_go//:fastcdc-go"],
)
