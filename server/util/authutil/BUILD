load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "authutil",
    srcs = ["authutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/authutil",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/util/alert",
        "//server/util/role",
        "//server/util/status",
        "@go_googleapis//google/rpc:errdetails_go_proto",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
    ],
)
