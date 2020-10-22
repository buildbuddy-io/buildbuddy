load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["invocationdb.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/invocationdb",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:acl_go_proto",
        "//proto:telemetry_go_proto",
        "//proto:user_id_go_proto",
        "//server/environment:go_default_library",
        "//server/tables:go_default_library",
        "//server/util/db:go_default_library",
        "//server/util/perms:go_default_library",
        "//server/util/query_builder:go_default_library",
        "//server/util/status:go_default_library",
        "@com_github_jinzhu_gorm//:go_default_library",
    ],
)
