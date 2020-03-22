load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "action_cache_server",
    srcs = ["action_cache_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server",
    deps = [
        ":digest",
        "//proto:remote_execution_go_proto",
        "//server:environment",
        "//server/util:status",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@go_googleapis//google/rpc:status_go_proto",
        "@org_golang_google_grpc//codes:go_default_library",
        "@org_golang_google_grpc//status:go_default_library",
    ],
)

go_library(
    name = "byte_stream_server",
    srcs = ["byte_stream_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server",
    deps = [
        "//server:environment",
        "//server:interfaces",
        "//server/util:status",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
    ],
)

go_library(
    name = "capabilities_server",
    srcs = ["capabilities_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/capabilities_server",
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:semver_go_proto",
    ],
)

go_library(
    name = "content_addressable_storage_server",
    srcs = ["content_addressable_storage_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server",
    deps = [
        ":digest",
        "//proto:remote_execution_go_proto",
        "//server:environment",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@go_googleapis//google/rpc:status_go_proto",
        "@org_golang_google_grpc//codes:go_default_library",
        "@org_golang_google_grpc//status:go_default_library",
    ],
)

go_library(
    name = "digest",
    srcs = [
        "digest.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest",
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/util:status",
    ],
)
