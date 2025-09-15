load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")

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
        "//cli/watcher",
        "//cli/watcher/option_definitions",
        "//server/util/lib/seq",
        "//server/util/rlimit",
        "//server/util/status",
    ],
)

go_test(
    name = "bb_test",
    srcs = ["bb_test.go"],
    deps = [
        ":bb_lib",
        "//cli/cli_command/register",
        "@com_github_google_go_cmp//cmp",
        "@com_github_stretchr_testify//assert",
    ],
)

go_binary(
    name = "bb",
    embed = [":bb_lib"],
    visibility = ["//visibility:public"],
)

go_binary(
    name = "bb-darwin-amd64",
    srcs = ["main.go"],
    out = "bb-darwin-amd64",
    gc_linkopts = [
        "-s",
        "-w",
    ],
    goarch = "amd64",
    goos = "darwin",
    pure = "on",
    tags = ["manual"],
    visibility = ["//visibility:public"],
    deps = [":bb_lib"],
)

go_binary(
    name = "bb-darwin-arm64",
    srcs = ["main.go"],
    out = "bb-darwin-arm64",
    gc_linkopts = [
        "-s",
        "-w",
    ],
    goarch = "arm64",
    goos = "darwin",
    pure = "on",
    tags = ["manual"],
    visibility = ["//visibility:public"],
    deps = [":bb_lib"],
)

go_binary(
    name = "bb-linux-amd64",
    srcs = ["main.go"],
    out = "bb-linux-amd64",
    gc_linkopts = [
        "-s",
        "-w",
    ],
    goarch = "amd64",
    goos = "linux",
    pure = "on",
    tags = ["manual"],
    visibility = ["//visibility:public"],
    deps = [":bb_lib"],
)

go_binary(
    name = "bb-linux-arm64",
    srcs = ["main.go"],
    out = "bb-linux-arm64",
    gc_linkopts = [
        "-s",
        "-w",
    ],
    goarch = "arm64",
    goos = "linux",
    pure = "on",
    tags = ["manual"],
    visibility = ["//visibility:public"],
    deps = [":bb_lib"],
)

go_binary(
    name = "bb-windows-amd64",
    srcs = ["main.go"],
    out = "bb-windows-amd64.exe",
    goarch = "amd64",
    goos = "windows",
    pure = "on",
    tags = ["manual"],
    visibility = ["//visibility:public"],
    deps = [":bb_lib"],
)

package(default_visibility = ["//cli:__subpackages__"])
