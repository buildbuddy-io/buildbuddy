load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "ssl",
    srcs = ["ssl.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/ssl",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/interfaces",
        "//server/util/status",
        "@org_golang_google_grpc//credentials",
        "@org_golang_x_crypto//acme",
        "@org_golang_x_crypto//acme/autocert",
    ],
)
