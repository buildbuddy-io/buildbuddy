load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "exptest",
    testonly = True,
    srcs = ["exptest.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/exptest",
    deps = [
        "//enterprise/server/experiments",
        "//server/real_environment",
        "//server/util/expflag",
        "@com_github_open_feature_go_sdk//openfeature",
        "@com_github_stretchr_testify//require",
    ],
)

go_test(
    name = "exptest_test",
    srcs = ["exptest_test.go"],
    deps = [
        ":exptest",
        "//server/real_environment",
        "//server/util/expflag",
        "@com_github_open_feature_go_sdk//openfeature",
        "@com_github_open_feature_go_sdk//openfeature/memprovider",
        "@com_github_stretchr_testify//require",
    ],
)
