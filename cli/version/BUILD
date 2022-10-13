load("@bazel_skylib//rules:common_settings.bzl", "string_flag")
load("@io_bazel_rules_go//go:def.bzl", "go_library")
load(":defs.bzl", "write_version_src")

string_flag(
    name = "cli_version",
    build_setting_default = "unknown",
)

# TODO(bduffany): This can be simplified a bit once go:embed supports
# generated files: https://github.com/bazelbuild/rules_go/pull/3285
write_version_src(
    name = "version_src",
    out = "version_flag.go",
    flag = ":cli_version",
)

go_library(
    name = "version_lib",
    srcs = [":version_src"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/version",
)

go_library(
    name = "version",
    srcs = ["version.go"],  # keep
    embed = [":version_lib"],  # keep
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/version",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
    ],
)
