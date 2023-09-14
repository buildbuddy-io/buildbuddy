load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "golang",
    srcs = ["golang.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/fix/golang",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/log",
        "@bazel_gazelle//language:go_default_library",
        "@bazel_gazelle//language/go:go_default_library",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])
