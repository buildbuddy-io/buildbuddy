load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "common",
    srcs = ["common.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/flagutil/common",
    visibility = [
        "//server/util/flagutil:__subpackages__",
        "//server/util/testing/flags:__subpackages__",
    ],
    deps = [
        "//server/util/status",
        "@in_gopkg_yaml_v3//:yaml_v3",
    ],
)

go_test(
    name = "common_test",
    size = "small",
    srcs = ["common_test.go"],
    deps = [
        ":common",
        "//server/util/flagutil/types",
        "//server/util/flagutil/types/autoflags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
