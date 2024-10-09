load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "proxy_peers",
    srcs = ["proxy_peers.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/proxy_peers",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/remote_cache/digest",
        "//server/util/flag",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/rangemap",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
    ],
)
