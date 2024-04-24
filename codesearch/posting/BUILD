load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "posting",
    srcs = ["posting.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/posting",
    visibility = ["//visibility:public"],
)

go_test(
    name = "posting_test",
    srcs = ["posting_test.go"],
    deps = [
        ":posting",
        "@com_github_stretchr_testify//assert",
    ],
)
