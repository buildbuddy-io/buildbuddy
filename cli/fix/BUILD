load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "fix",
    srcs = ["fix.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/fix",
    # keep
    deps = [
        ":gazelle",
        ":langs",
        ":language",
        "//cli/add",
        "//cli/arg",
        "//cli/log",
        "//cli/translate",
        "//cli/workspace",
        "//server/util/flag",
        "@com_github_bazelbuild_buildtools//buildifier:buildifier_lib",
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
        "//cli/fix/golang",
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

go_library(
    name = "langs",
    srcs = ["langs.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/fix/langs",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/fix/golang",
        "//cli/fix/typescript",
        "@bazel_gazelle//language",
        "@bazel_gazelle//language/bazel/visibility",
        "@bazel_gazelle//language/go",
        "@bazel_gazelle//language/proto",
    ],
)

go_library(
    name = "language",
    srcs = ["language.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/fix/language",
    visibility = ["//visibility:public"],
)

package(default_visibility = ["//cli:__subpackages__"])
