load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "signedrelease",
    srcs = ["signedrelease.go"],
    embedsrcs = ["release_signing_public_key.pem"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/signedrelease",
)

go_test(
    name = "signedrelease_test",
    size = "small",
    srcs = ["signedrelease_test.go"],
    embed = [":signedrelease"],
    deps = ["@com_github_stretchr_testify//require"],
)
