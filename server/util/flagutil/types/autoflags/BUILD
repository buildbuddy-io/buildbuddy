load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "autoflags",
    srcs = ["autoflags.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/types/autoflags",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/flagutil/common",
        "//server/util/flagutil/types",
    ],
)

go_test(
    name = "autoflags_test",
    srcs = ["autoflags_test.go"],
    embed = [":autoflags"],
    deps = [
        "//server/util/flagutil/common",
        "//server/util/flagutil/types",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
