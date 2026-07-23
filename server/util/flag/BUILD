load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "flag",
    srcs = ["flag.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/flag",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/flagutil/common",
        "//server/util/flagutil/types",
        "//server/util/flagutil/types/autoflags",
        "//server/util/flagutil/types/autoflags/tags",
    ],
)

go_test(
    name = "flag_test",
    size = "small",
    srcs = ["flag_test.go"],
    deps = [
        ":flag",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
