load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "cdc",
    srcs = ["cdc.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/cdc",
    visibility = ["//visibility:public"],
    deps = ["@org_golang_google_grpc//metadata"],
)
