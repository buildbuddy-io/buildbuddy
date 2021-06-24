load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "perms",
    srcs = ["perms.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/perms",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:acl_go_proto",
        "//proto:context_go_proto",
        "//proto:user_id_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/log",
        "//server/util/query_builder",
        "//server/util/request_context",
        "//server/util/status",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@org_golang_google_protobuf//proto",
    ],
)
