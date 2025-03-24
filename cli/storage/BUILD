load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "storage",
    srcs = ["storage.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/storage",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/workspace",
        "//server/util/hash",
        "//server/util/uuid",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])
