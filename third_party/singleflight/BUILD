load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "singleflight",
    srcs = ["singleflight.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/third_party/singleflight",
    visibility = ["//visibility:public"],
)

go_test(
    name = "singleflight_test",
    srcs = ["singleflight_test.go"],
    embed = [":singleflight"],
    deps = ["@com_github_stretchr_testify//require"],
)
