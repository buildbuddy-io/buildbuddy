load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "typescript",
    srcs = ["typescript.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/fix/typescript",
    deps = [
        "@bazel_gazelle//config",
        "@bazel_gazelle//label",
        "@bazel_gazelle//language",
        "@bazel_gazelle//repo",
        "@bazel_gazelle//resolve",
        "@bazel_gazelle//rule",
        "@com_github_smacker_go_tree_sitter//:go-tree-sitter",
        "@com_github_smacker_go_tree_sitter//typescript/tsx",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])
