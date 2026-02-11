load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "envfile",
    srcs = ["envfile.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/ci_runner/envfile",
)

go_test(
    name = "envfile_test",
    srcs = ["envfile_test.go"],
    deps = [
        ":envfile",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
