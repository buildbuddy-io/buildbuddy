load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "log",
    srcs = ["log.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/log",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/status",
        "@com_github_rs_zerolog//:zerolog",
        "@com_github_rs_zerolog//log",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
    ],
)
