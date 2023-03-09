load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "typescript",
    srcs = ["typescript.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/fix/typescript",
    visibility = ["//visibility:public"],
    deps = [
        "@bazel_gazelle//config:go_default_library",
        "@bazel_gazelle//label:go_default_library",
        "@bazel_gazelle//language:go_default_library",
        "@bazel_gazelle//repo:go_default_library",
        "@bazel_gazelle//resolve:go_default_library",
        "@bazel_gazelle//rule:go_default_library",
        "@com_github_smacker_go_tree_sitter//:go-tree-sitter",
        "@com_github_smacker_go_tree_sitter//typescript/tsx",
    ],
)
