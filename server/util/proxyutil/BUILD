load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "proxyutil",
    srcs = ["proxyutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/proxyutil",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//test/bufconn",
    ],
)
