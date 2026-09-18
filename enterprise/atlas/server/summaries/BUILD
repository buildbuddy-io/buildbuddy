load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "summaries",
    srcs = [
        "query.go",
        "summaries.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/summaries",
    visibility = ["//visibility:public"],
)

go_test(
    name = "summaries_test",
    srcs = ["summaries_test.go"],
    embed = [":summaries"],
    deps = ["@com_github_stretchr_testify//require"],
)
