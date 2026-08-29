load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")

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
    # The CLI has no cgo dependencies, so build it with CGO_ENABLED=0. This
    # yields a fully static binary on Linux (which the copy embedded in the
    # executor, //server/util/bb, needs inside workflow containers) and lets
    # it share one Bazel configuration -- one Go stdlib and one compile of
    # every common package -- with the other pure-Go binaries embedded in the
    # executor (goinit, ci_runner). The release targets below are pure too.
    pure = "on",
    visibility = ["//visibility:public"],
)

go_binary(
    name = "bb-darwin-amd64",
    out = "bb-darwin-amd64",
    embed = [":bb_lib"],
    gc_linkopts = [
        "-s",
        "-w",
    ],
    goarch = "amd64",
    goos = "darwin",
    pure = "on",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

go_binary(
    name = "bb-darwin-arm64",
    out = "bb-darwin-arm64",
    embed = [":bb_lib"],
    gc_linkopts = [
        "-s",
        "-w",
    ],
    goarch = "arm64",
    goos = "darwin",
    pure = "on",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

go_binary(
    name = "bb-linux-amd64",
    out = "bb-linux-amd64",
    embed = [":bb_lib"],
    gc_linkopts = [
        "-s",
        "-w",
    ],
    goarch = "amd64",
    goos = "linux",
    pure = "on",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

go_binary(
    name = "bb-linux-arm64",
    out = "bb-linux-arm64",
    embed = [":bb_lib"],
    gc_linkopts = [
        "-s",
        "-w",
    ],
    goarch = "arm64",
    goos = "linux",
    pure = "on",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

go_binary(
    name = "bb-windows-amd64",
    out = "bb-windows-amd64.exe",
    embed = [":bb_lib"],
    goarch = "amd64",
    goos = "windows",
    pure = "on",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

go_test(
    name = "bb_test",
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
