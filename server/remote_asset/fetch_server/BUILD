load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "fetch_server",
    srcs = ["fetch_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_asset/fetch_server",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_asset_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/real_environment",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/log",
        "//server/util/prefix",
        "//server/util/scratchspace",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_genproto_googleapis_rpc//status",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_protobuf//types/known/durationpb",
    ],
)
