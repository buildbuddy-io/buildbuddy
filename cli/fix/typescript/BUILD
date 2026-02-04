load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = [
    "//:__pkg__",  # keep
    "//cli:__subpackages__",
])

go_library(
    name = "typescript",
    srcs = [
        "typescript.go",
        "typescript_pure.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/fix/typescript",
    deps = [
        "@bazel_gazelle//config",
        "@bazel_gazelle//label",
        "@bazel_gazelle//language",
        "@bazel_gazelle//repo",
        "@bazel_gazelle//resolve",
        "@bazel_gazelle//rule",
    ] + select({
        "//conditions:default": [
            "@com_github_smacker_go_tree_sitter//:go-tree-sitter",
            "@com_github_smacker_go_tree_sitter//typescript/tsx",
        ],
    }),
)
