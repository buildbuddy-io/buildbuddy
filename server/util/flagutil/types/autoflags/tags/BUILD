load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "tags",
    srcs = ["tags.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types/autoflags/tags",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/flagutil/common",
        "//server/util/flagutil/yaml",
        "//server/util/log",
    ],
)

go_test(
    name = "tags_test",
    srcs = ["tags_test.go"],
    embed = [":tags"],
    deps = [
        "//server/util/flagutil/common",
        "//server/util/flagutil/yaml",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@in_gopkg_yaml_v3//:yaml_v3",
    ],
)
