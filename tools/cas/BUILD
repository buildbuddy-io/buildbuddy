load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "cas_lib",
    srcs = ["cas.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/cas",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:buildbuddy_service_go_proto",
        "//proto:cache_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
        "@com_github_mattn_go_isatty//:go-isatty",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//encoding/protojson",
    ],
)

go_binary(
    name = "cas",
    embed = [":cas_lib"],
    visibility = ["//visibility:public"],
)
