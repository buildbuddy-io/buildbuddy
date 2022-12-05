load("@bazel_skylib//rules:common_settings.bzl", "string_flag")
load("@io_bazel_rules_go//go:def.bzl", "go_library")
load("//rules/flags:index.bzl", "write_flag_to_file")

string_flag(
    name = "cli_version",
    build_setting_default = "unknown",
)

# TODO(bduffany): This can be simplified a bit once go:embed supports
# generated files: https://github.com/bazelbuild/rules_go/pull/3285
write_flag_to_file(
    name = "version_flag.go",
    flag = ":cli_version",
    template = """package version

var cliVersionFlag = "%s"
""",
)

go_library(
    name = "version_lib",
    srcs = [":version_flag.go"],
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
