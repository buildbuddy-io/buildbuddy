load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["ssl.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/ssl",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/environment:go_default_library",
        "//server/interfaces:go_default_library",
        "//server/util/status:go_default_library",
        "@org_golang_google_grpc//credentials:go_default_library",
        "@org_golang_x_crypto//acme:go_default_library",
        "@org_golang_x_crypto//acme/autocert:go_default_library",
    ],
)
