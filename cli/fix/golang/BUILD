load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "golang",
    srcs = ["golang.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/fix/golang",
    visibility = ["//visibility:public"],
    deps = [
        "//bazel-gazelle/language",
        "//bazel-gazelle/language/go",
        "//cli/log",
        "//cli/workspace",
        "@org_golang_x_mod//modfile",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])
