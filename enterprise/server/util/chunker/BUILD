load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "chunker",
    srcs = ["chunker.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/chunker",
    deps = [
        "//server/util/status",
        "@com_github_jotfs_fastcdc_go//v2020:fastcdc-go",
    ],
)

go_test(
    name = "chunker_test",
    srcs = ["chunker_test.go"],
    deps = [
        ":chunker",
        "@com_github_stretchr_testify//require",
    ],
)
