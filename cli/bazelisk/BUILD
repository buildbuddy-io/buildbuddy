load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

# gazelle:resolve go github.com/bazelbuild/bazelisk/core @com_github_bazelbuild_bazelisk//core:go_default_library
# gazelle:resolve go github.com/bazelbuild/bazelisk/repositories @com_github_bazelbuild_bazelisk//repositories:go_default_library

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "bazelisk",
    srcs = [
        "bazel_release_key.go",
        "bazelisk.go",
        "verify.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/bazelisk",
    deps = [
        "//cli/log",
        "//cli/workspace",
        "@com_github_bazelbuild_bazelisk//config",
        "@com_github_bazelbuild_bazelisk//core:go_default_library",
        "@com_github_bazelbuild_bazelisk//httputil",
        "@com_github_bazelbuild_bazelisk//platforms",
        "@com_github_bazelbuild_bazelisk//repositories:go_default_library",
        "@com_github_protonmail_go_crypto//openpgp",
        "@com_github_protonmail_go_crypto//openpgp/armor",
        "@com_github_protonmail_go_crypto//openpgp/errors",
    ],
)

go_test(
    name = "bazelisk_test",
    srcs = ["verify_test.go"],
    data = glob(["testdata/**"]),
    embed = [":bazelisk"],
    deps = [
        "@com_github_bazelbuild_bazelisk//config",
        "@com_github_bazelbuild_bazelisk//core:go_default_library",
        "@com_github_protonmail_go_crypto//openpgp",
        "@com_github_protonmail_go_crypto//openpgp/errors",
        "@com_github_protonmail_go_crypto//openpgp/packet",
        "@com_github_stretchr_testify//require",
    ],
)
