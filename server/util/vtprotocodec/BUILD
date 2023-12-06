load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "vtprotocodec",
    srcs = ["vtprotocodec.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/vtprotocodec",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/proto",
        "@org_golang_google_grpc//encoding",
        "@org_golang_google_grpc//encoding/proto",
    ],
)
