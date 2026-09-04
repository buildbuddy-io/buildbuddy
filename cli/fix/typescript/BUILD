load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = [
    "//:__pkg__",  # keep
    "//cli:__subpackages__",
])

go_library(
    name = "typescript",
    srcs = ["typescript.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/fix/typescript",
    deps = [
        "@bazel_gazelle//language",
        "@bazel_gazelle//repo",
        "@bazel_gazelle//resolve",
        "@bazel_gazelle//v2/config",
        "@bazel_gazelle//v2/label",
        "@bazel_gazelle//v2/resolve",
        "@bazel_gazelle//v2/rule",
        "//cli/fix/typescript/tsimports",
    ],
)
