load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["content_addressable_storage_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:api_key_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/environment:go_default_library",
        "//server/interfaces:go_default_library",
        "//server/remote_cache/digest:go_default_library",
        "//server/remote_cache/hit_tracker:go_default_library",
        "//server/remote_cache/namespace:go_default_library",
        "//server/util/capabilities:go_default_library",
        "//server/util/prefix:go_default_library",
        "//server/util/status:go_default_library",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@go_googleapis//google/rpc:status_go_proto",
        "@org_golang_google_grpc//codes:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = ["content_addressable_storage_server_test.go"],
    embed = [":go_default_library"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/backends/memory_cache:go_default_library",
        "//server/interfaces:go_default_library",
        "//server/testutil/testdigest:go_default_library",
        "//server/testutil/testenv:go_default_library",
        "//server/util/prefix:go_default_library",
        "@com_github_stretchr_testify//assert:go_default_library",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//codes:go_default_library",
    ],
)
