load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "keystore",
    srcs = ["keystore.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/keystore",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/util/status",
        "@org_golang_x_crypto//nacl/box",
    ],
)
