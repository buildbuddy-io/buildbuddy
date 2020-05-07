load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["status.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/status",
    visibility = ["//visibility:public"],
    deps = [
        "@org_golang_google_grpc//codes:go_default_library",
        "@org_golang_google_grpc//status:go_default_library",
    ],
)
