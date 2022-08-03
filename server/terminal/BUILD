load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "terminal",
    srcs = [
        "element.go",
        "node.go",
        "output.go",
        "parser.go",
        "screen.go",
        "style.go",
        "terminal.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/terminal",
    visibility = ["//visibility:public"],
    deps = ["//server/util/log"],
)

go_test(
    name = "terminal_test",
    size = "small",
    srcs = ["terminal_test.go"],
    deps = [
        ":terminal",
        "//server/util/random",
    ],
)
