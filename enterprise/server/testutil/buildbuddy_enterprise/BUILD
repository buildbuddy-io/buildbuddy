load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "buildbuddy_enterprise",
    testonly = 1,
    srcs = ["buildbuddy_enterprise.go"],
    data = [
        "//enterprise:config/buildbuddy.local.yaml",
        "//enterprise/server/cmd/server:buildbuddy",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/buildbuddy_enterprise",
    visibility = ["//visibility:public"],
    deps = ["//server/testutil/app"],
)
