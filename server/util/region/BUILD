load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "region",
    srcs = ["region.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/region",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:config_go_proto",
        "//server/util/flag",
    ],
)

go_test(
    name = "region_test",
    srcs = ["region_test.go"],
    embed = [":region"],
    deps = ["@com_github_stretchr_testify//assert"],
)
