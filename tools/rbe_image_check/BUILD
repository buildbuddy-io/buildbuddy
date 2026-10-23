load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")

go_library(
    name = "rbe_image_check_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/rbe_image_check",
    visibility = ["//visibility:private"],
)

go_binary(
    name = "rbe_image_check",
    embed = [":rbe_image_check_lib"],
    pure = "on",
    static = "on",
    visibility = ["//visibility:public"],
)

go_test(
    name = "rbe_image_check_test",
    srcs = ["main_test.go"],
    embed = [":rbe_image_check_lib"],
    deps = ["@com_github_stretchr_testify//require"],
)
