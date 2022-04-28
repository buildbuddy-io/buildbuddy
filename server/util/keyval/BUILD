load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "keyval",
    srcs = ["keyval.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/keyval",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "@org_golang_google_protobuf//proto",
    ],
)
