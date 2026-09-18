load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "stats",
    srcs = ["stats.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/stats",
    deps = ["//proto:stats_go_proto"],
)
