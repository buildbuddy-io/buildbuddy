load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")
load("@bazel_skylib//rules:common_settings.bzl", "string_flag")
load("//rules/flags:index.bzl", "write_flag_to_file")

string_flag(
    name = "cli_version",
    build_setting_default = "unknown",
)

write_flag_to_file(
    name = "version_flag",
    out = "version_flag.txt",
    flag = ":cli_version",
)

go_library(
    name = "version",
    srcs = ["version.go"],
    embedsrcs = [":version_flag"],  # keep
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/version",
    deps = ["//cli/arg"],
)

go_test(
    name = "version_test",
    srcs = ["version_test.go"],
    deps = [
        ":version",
        "@com_github_stretchr_testify//require",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])
