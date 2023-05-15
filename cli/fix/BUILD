load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "fix",
    srcs = ["fix.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/fix",
    # keep
    deps = [
        ":gazelle",
        "//cli/arg",
        "//cli/log",
        "@com_github_bazelbuild_buildtools//buildifier:go_default_library",
    ],
)

go_library(
    name = "gazelle",
    # keep
    srcs = [
        "langs.go",
        "@bazel_gazelle//cmd/gazelle:diff.go",
        "@bazel_gazelle//cmd/gazelle:fix.go",
        "@bazel_gazelle//cmd/gazelle:fix-update.go",
        "@bazel_gazelle//cmd/gazelle:gazelle.go",
        "@bazel_gazelle//cmd/gazelle:metaresolver.go",
        "@bazel_gazelle//cmd/gazelle:print.go",
        "@bazel_gazelle//cmd/gazelle:update-repos.go",
    ],
    importpath = "github.com/bazelbuild/bazel-gazelle/cmd/gazelle",
    tags = ["manual"],
    deps = [
        "//cli/fix/typescript",
        "@bazel_gazelle//config",
        "@bazel_gazelle//flag",
        "@bazel_gazelle//internal/module",
        "@bazel_gazelle//internal/wspace",
        "@bazel_gazelle//label",
        "@bazel_gazelle//language",
        "@bazel_gazelle//language/bazel/visibility",
        "@bazel_gazelle//language/go",
        "@bazel_gazelle//language/proto",
        "@bazel_gazelle//merger",
        "@bazel_gazelle//repo",
        "@bazel_gazelle//resolve",
        "@bazel_gazelle//rule",
        "@bazel_gazelle//walk",
        "@com_github_pmezard_go_difflib//difflib",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])
