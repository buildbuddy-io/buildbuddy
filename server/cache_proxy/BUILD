load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "cache_proxy",
    srcs = ["cache_proxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/cache_proxy",
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:semver_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/real_environment",
        "//server/remote_cache/byte_stream_server",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/content_addressable_storage_server",
        "//server/remote_cache/digest",
        "//server/remote_cache/hit_tracker",
        "//server/util/bazel_request",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//test/bufconn",
        "@org_golang_google_protobuf//encoding/protojson",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])
