load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")
load("//rules:platform_transitions.bzl", "linux_arm64_alias", "linux_x86_64_alias", "macos_arm64_alias", "macos_x86_64_alias", "windows_amd64_alias")

# NOTE: //tools/lint uses a prebuilt bb binary (//tools/bb) instead of this
# target. If you make CLI changes that affect `bb fix` or `bb mod deps`,
# run `bazel run //tools/bb:update_version` after your release to pick them up.

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "bb_lib",
    srcs = ["bb.go"],
    data = [
        "//cli/cmd/sidecar",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/cmd/bb",
    visibility = ["//visibility:private"],
    deps = [
        "//cli/arg",
        "//cli/bazelisk",
        "//cli/cli_command",
        "//cli/cli_command/register",
        "//cli/cmd/sidecar",
        "//cli/help",
        "//cli/help/option_definitions",
        "//cli/log",
        "//cli/log/option_definitions",
        "//cli/metadata",
        "//cli/parser",
        "//cli/parser/arguments",
        "//cli/parser/bbrc",
        "//cli/parser/options",
        "//cli/parser/parsed",
        "//cli/picker",
        "//cli/plugin",
        "//cli/runscript",
        "//cli/setup",
        "//cli/stream_run_logs",
        "//cli/stream_run_logs/option_definitions",
        "//cli/watcher",
        "//cli/watcher/option_definitions",
        "//server/util/lib/seq",
        "//server/util/rlimit",
        "//server/util/status",
    ],
)

go_binary(
    name = "bb",
    embed = [":bb_lib"],
    visibility = ["//visibility:public"],
)

macos_x86_64_alias(
    name = "bb-darwin-amd64",
    actual = ":bb",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

macos_arm64_alias(
    name = "bb-darwin-arm64",
    actual = ":bb",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

linux_x86_64_alias(
    name = "bb-linux-amd64",
    actual = ":bb",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

linux_arm64_alias(
    name = "bb-linux-arm64",
    actual = ":bb",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

windows_amd64_alias(
    name = "bb-windows-amd64",
    actual = ":bb",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

go_test(
    name = "bb_test",
    size = "small",
    srcs = ["bb_test.go"],
    data = [":bb"],
    embed = [":bb_lib"],
    x_defs = {
        "bbRunfilePath": "$(rlocationpath :bb)",
    },
    deps = [
        "//server/testutil/testfs",
        "@com_github_creack_pty//:pty",
        "@com_github_stretchr_testify//require",
    ],
)
