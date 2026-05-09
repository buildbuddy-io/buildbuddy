load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "fix",
    srcs = ["fix.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/fix",
    # keep
    deps = [
        "//cli/add",
        "//cli/arg",
        "//cli/bazelisk",
        "//cli/fix/langs:gazelle",
        "//cli/fix/language",
        "//cli/log",
        "//cli/translate",
        "//cli/workspace",
        "//server/util/flag",
        "@com_github_bazelbuild_buildtools//buildifier:buildifier_lib",
        "@com_github_bazelbuild_buildtools//edit",
    ],
)

go_test(
    name = "fix_test",
    srcs = ["fix_internal_test.go"],
    embed = [":fix"],
)
