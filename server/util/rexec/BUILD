load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "rexec",
    srcs = ["rexec.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/rexec",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/retry",
        "//server/util/status",
        "@org_golang_google_genproto//googleapis/longrunning",
        "@org_golang_google_grpc//status",
        "@org_golang_x_exp//maps",
        "@org_golang_x_sync//errgroup",
    ],
)
