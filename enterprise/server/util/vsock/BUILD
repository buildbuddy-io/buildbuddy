load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "vsock",
    srcs = ["vsock.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/vsock",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/status",
        "@com_github_mdlayher_vsock//:vsock",
        "@org_golang_x_sys//unix",
    ],
)
