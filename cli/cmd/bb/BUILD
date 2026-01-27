load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

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
