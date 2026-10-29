load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

# gazelle:resolve go github.com/bazelbuild/bazelisk/core @com_github_bazelbuild_bazelisk//core:go_default_library
# gazelle:resolve go github.com/bazelbuild/bazelisk/repositories @com_github_bazelbuild_bazelisk//repositories:go_default_library

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "bazelisk",
    srcs = [
        "bazelisk.go",
        "signed_fork_repo.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/bazelisk",
    deps = [
        "//cli/log",
        "//cli/signedrelease",
        "//cli/workspace",
        "@com_github_bazelbuild_bazelisk//config",
        "@com_github_bazelbuild_bazelisk//core:go_default_library",
        "@com_github_bazelbuild_bazelisk//platforms",
        "@com_github_bazelbuild_bazelisk//repositories:go_default_library",
    ],
)

go_test(
    name = "signed_fork_repo_test",
    size = "small",
    srcs = ["signed_fork_repo_test.go"],
    embed = [":bazelisk"],
    deps = [
        "//cli/signedrelease",
        "@com_github_bazelbuild_bazelisk//config",
        "@com_github_bazelbuild_bazelisk//core:go_default_library",
        "@com_github_stretchr_testify//require",
    ],
)

go_test(
    name = "bazelisk_test",
    srcs = ["signed_fork_repo_test.go"],
    embed = [":bazelisk"],
    deps = [
        "//cli/signedrelease",
        "@com_github_bazelbuild_bazelisk//config",
        "@com_github_bazelbuild_bazelisk//core:go_default_library",
        "@com_github_stretchr_testify//require",
    ],
)
