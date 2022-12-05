load("@bazel_skylib//rules:common_settings.bzl", "string_flag")
load("@io_bazel_rules_go//go:def.bzl", "go_library")
load("//rules/flags:index.bzl", "write_flag_to_file")

# Define a build flag that sets the embedded version tag:
#
#     bazel build server --//server/version:version_tag=v1.2.3
string_flag(
    name = "version_tag",
    build_setting_default = "",
)

go_library(
    name = "version",
    srcs = ["version.go"],  # keep
    embed = [":version_tag_lib"],  # keep
    importpath = "github.com/buildbuddy-io/buildbuddy/server/version",
    visibility = ["//visibility:public"],
    x_defs = {
        "commitSha": "{COMMIT_SHA}",
    },
    deps = [
        "//server/util/log",
    ],
)

go_library(
    name = "version_tag_lib",
    srcs = [":version_tag.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/version",
)

# TODO(bduffany): Use go:embed instead of a generated go library once generated
# files can be embedded: https://github.com/bazelbuild/rules_go/pull/3285
write_flag_to_file(
    name = "version_tag.go",
    flag = ":version_tag",
    template = """package version

var versionTag = "%s"
""",
)
