load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "mdutil",
    srcs = ["mdutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/mdutil",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/status",
        "@org_golang_google_grpc//metadata",
    ],
)
