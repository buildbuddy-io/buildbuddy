load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "error_util",
    srcs = ["error_util.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/error_util",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/alert",
        "@org_golang_google_genproto_googleapis_rpc//errdetails",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
    ],
)
