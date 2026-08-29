load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "gitclient",
    srcs = ["gitclient.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/gitclient",
    visibility = [
        "//cli/index:__pkg__",
        "//codesearch:__subpackages__",
    ],
    deps = [
        "//proto:index_go_proto",
        "//server/util/log",
        "//server/util/status",
    ],
)

go_test(
    name = "gitclient_test",
    srcs = ["gitclient_test.go"],
    embed = [":gitclient"],
    deps = [
        "//proto:index_go_proto",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
