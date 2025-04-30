load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "golang",
    srcs = ["golang.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/fix/golang",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/log",
        "//cli/workspace",
        "@bazel_gazelle//label",
        "@bazel_gazelle//language",
        "@bazel_gazelle//language/go",
        "@org_golang_x_mod//modfile",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])
