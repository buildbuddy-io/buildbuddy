load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

filegroup(
    name = "ratings_data",
    srcs = glob([
        "ratings.json",
    ]),
)

go_library(
    name = "rater_lib",
    srcs = ["rater.go"],
    embedsrcs = ["ratings.json"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/test/quality",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:codesearch_service_go_proto",
        "//proto:index_go_proto",
        "//proto:search_go_proto",
        "//proto:search_rating_go_proto",
        "//server/util/grpc_client",
        "//server/util/log",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//encoding/protojson",
    ],
)

go_binary(
    name = "rater",
    embed = [":rater_lib"],
    visibility = ["//visibility:public"],
)
