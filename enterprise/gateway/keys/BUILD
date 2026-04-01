load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "keys",
    srcs = ["keys.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/gateway/keys",
    visibility = ["//visibility:public"],
    deps = [
        "@org_golang_x_crypto//curve25519",
    ],
)

go_test(
    name = "keys_test",
    srcs = ["keys_test.go"],
    embed = [":keys"],
    deps = [
        "@com_github_stretchr_testify//require",
    ],
)
