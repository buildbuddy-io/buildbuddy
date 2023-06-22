load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "yaml",
    srcs = ["yaml.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/yaml",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/alert",
        "//server/util/flagutil/common",
        "//server/util/log",
        "//server/util/status",
        "@in_gopkg_yaml_v3//:yaml_v3",
    ],
)

go_test(
    name = "yaml_test",
    size = "small",
    srcs = ["yaml_test.go"],
    deps = [
        ":yaml",
        "//server/util/flagutil/common",
        "//server/util/flagutil/types",
        "@com_github_google_go_cmp//cmp",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@in_gopkg_yaml_v3//:yaml_v3",
    ],
)
