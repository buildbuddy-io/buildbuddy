load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "server_lib",
    srcs = ["server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/cmd/server",
    visibility = ["//visibility:private"],
    deps = [
        "//codesearch/index",
        "//codesearch/query",
        "//codesearch/regexp",
        "//proto/codesearch:codesearch_service_go_proto",
        "//proto/codesearch:index_go_proto",
        "//proto/codesearch:search_go_proto",
        "@com_github_cockroachdb_pebble//:pebble",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//reflection",
    ],
)

go_binary(
    name = "server",
    embed = [":server_lib"],
    visibility = ["//visibility:public"],
)
