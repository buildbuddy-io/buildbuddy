load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "tsimports",
    srcs = ["tsimports.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/fix/typescript/tsimports",
    visibility = ["//cli:__subpackages__"],
)

go_test(
    name = "tsimports_test",
    srcs = ["tsimports_test.go"],
    embed = [":tsimports"],
    deps = ["@com_github_stretchr_testify//require"],
)
