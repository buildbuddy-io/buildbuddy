load("@io_bazel_rules_go//go:def.bzl", "go_library")

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
        "//cli/fix/langs:langs",
        "//cli/fix/language:language",
        "//cli/log",
        "//cli/translate",
        "//cli/workspace",
        "//server/util/flag",
        "@com_github_bazelbuild_buildtools//buildifier:buildifier_lib",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])
