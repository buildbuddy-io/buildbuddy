load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "wgkeys",
    srcs = ["wgkeys.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/wgkeys",
    visibility = ["//visibility:public"],
    deps = [
        "@org_golang_x_crypto//curve25519",
    ],
)

go_test(
    name = "wgkeys_test",
    srcs = ["wgkeys_test.go"],
    embed = [":wgkeys"],
    deps = [
        "@com_github_stretchr_testify//require",
    ],
)
