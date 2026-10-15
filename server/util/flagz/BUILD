load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "flagz",
    srcs = ["flagz.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/flagz",
    visibility = ["//visibility:public"],
    deps = ["//server/util/flagutil/yaml"],
)

go_test(
    name = "flagz_test",
    srcs = ["flagz_test.go"],
    deps = [
        ":flagz",
        "//server/util/flag",
        "//server/util/flagutil/yaml",
        "@com_github_stretchr_testify//require",
    ],
)
