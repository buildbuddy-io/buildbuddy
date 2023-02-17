load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "fix",
    srcs = ["fix.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/fix",
    visibility = ["//visibility:public"],
    deps = [
        ":gazelle",
        "//cli/arg",
        "//cli/log",
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
    visibility = ["//visibility:public"],
    deps = [
        "//cli/fix/typescript",
        "@bazel_gazelle//config",
        "@bazel_gazelle//flag",
        "@bazel_gazelle//internal/wspace",
        "@bazel_gazelle//label",
        "@bazel_gazelle//language",
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
