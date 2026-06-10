load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

# gazelle:exclude langs.go

go_library(
    name = "gazelle",
    # keep
    srcs = [
        "@bazel_gazelle//cmd/gazelle:main.go",
        "@bazel_gazelle//cmd/gazelle:update-repos.go",
        # Taken from @bazel_gazelle//cmd/gazelle/BUILD.bazel gazelle_lib
        "langs.go",
    ],
    importpath = "github.com/bazelbuild/bazel-gazelle/cmd/gazelle",
    tags = ["manual"],
    deps = [
        "//cli/fix/golang",
        "//cli/fix/typescript",
        "@bazel_gazelle//config",
        "@bazel_gazelle//internal/wspace",
        "@bazel_gazelle//label",
        "@bazel_gazelle//language",
        "@bazel_gazelle//language/bazel/visibility",
        "@bazel_gazelle//language/go",
        "@bazel_gazelle//language/proto",
        "@bazel_gazelle//merger",
        "@bazel_gazelle//repo",
        "@bazel_gazelle//rule",
        "@bazel_gazelle//v2/cmd/gazelle/update",
    ],
)
