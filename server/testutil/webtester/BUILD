load("@io_bazel_rules_go//go:def.bzl", "go_library")

# gazelle:resolve go github.com/bazelbuild/rules_webtesting/go/webtest @io_bazel_rules_webtesting//go/webtest:go_default_library

go_library(
    name = "webtester",
    testonly = 1,
    srcs = ["webtester.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/webtester",
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@com_github_tebeka_selenium//:selenium",
        "@com_github_tebeka_selenium//chrome",
        "@io_bazel_rules_webtesting//go/webtest:go_default_library",
    ],
)
