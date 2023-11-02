load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "debugutil",
    srcs = ["debugutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/debugutil",
    visibility = ["//visibility:public"],
    deps = ["@org_golang_google_grpc//metadata"],
)
