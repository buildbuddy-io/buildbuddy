load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
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
)

go_test(
    name = "go_default_test",
    srcs = ["terminal_test.go"],
    embed = [":go_default_library"],
    deps = ["//server/util/random:go_default_library"],
)
