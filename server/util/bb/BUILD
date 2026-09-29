load("@bazel_skylib//rules:copy_file.bzl", "copy_file")
load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("@io_bazel_rules_go//go:def.bzl", "go_library")
load("//rules:platform_transitions.bzl", "linux_arm64_musl_alias", "linux_x86_64_musl_alias")

# Embeds the bb CLI binary for Linux and mac. On Windows, we create an empty
# file instead because the CLI depends on tree-sitter which requires CGo, and
# our Windows build environment doesn't have a C compiler available.
copy_file(
    name = "bb-crossplatform",
    src = select({
        "//platforms/configs:linux_x86_64": ":bb-linux-x86_64",
        "//platforms/configs:linux_arm64": ":bb-linux-arm64",
        "//platforms/configs:macos_x86_64": "//cli/cmd/bb",
        "//platforms/configs:macos_arm64": "//cli/cmd/bb",
        "//conditions:default": ":empty-bb",
    }),
    out = "bb-bin",
    allow_symlink = True,
    visibility = ["//visibility:public"],
)

write_file(
    name = "empty-bb",
    out = "empty-bb-bin",
)

go_library(
    name = "bb",
    srcs = ["bb.go"],
    embedsrcs = ["bb-bin"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/bb",
    visibility = ["//visibility:public"],
)

linux_arm64_musl_alias(
    name = "bb-linux-arm64",
    actual = "//cli/cmd/bb",
    tags = ["manual"],
)

linux_x86_64_musl_alias(
    name = "bb-linux-x86_64",
    actual = "//cli/cmd/bb",
    tags = ["manual"],
)
