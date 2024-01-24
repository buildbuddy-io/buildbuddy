load("@rules_proto//proto:defs.bzl", "proto_library")
load("@io_bazel_rules_go//proto:compiler.bzl", "go_proto_compiler")
load("@io_bazel_rules_go//proto:def.bzl", "go_proto_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "index_proto",
    srcs = ["index.proto"],
    deps = [],
)

go_proto_library(
    name = "index_go_proto",
    importpath = "github.com/google/codesearch/proto/index",
    proto = ":index_proto",
)

proto_library(
    name = "search_proto",
    srcs = ["search.proto"],
    deps = [],
)

go_proto_library(
    name = "search_go_proto",
    importpath = "github.com/google/codesearch/proto/search",
    proto = ":search_proto",
)

proto_library(
    name = "codesearch_service_proto",
    srcs = ["codesearch_service.proto"],
    deps = [
        ":index_proto",
        ":search_proto",
    ],
)

go_proto_library(
    name = "codesearch_service_go_proto",
    compilers = [
        "@io_bazel_rules_go//proto:go_proto",
        "@io_bazel_rules_go//proto:go_grpc_v2",
    ],
    importpath = "github.com/google/codesearch/proto/codesearch_service",
    proto = ":codesearch_service_proto",
    deps = [
        ":index_go_proto",
        ":search_go_proto",
    ],
)
