load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "status",
    srcs = ["status.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/status",
    visibility = ["//visibility:public"],
    deps = [
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
    ],
)
