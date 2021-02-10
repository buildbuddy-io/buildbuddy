load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    testonly = 1,
    srcs = ["buildbuddy.go"],
    data = [
        "//:config/buildbuddy.local.yaml",
        "//server/cmd/buildbuddy",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/buildbuddy",
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_phayes_freeport//:go_default_library",
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
    ],
)
