load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "invocationdb",
    srcs = ["invocationdb.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/invocationdb",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:acl_go_proto",
        "//proto:api_key_go_proto",
        "//proto:invocation_go_proto",
        "//proto:telemetry_go_proto",
        "//proto:user_id_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/util/capabilities",
        "//server/util/db",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/query_builder",
        "//server/util/retry",
        "//server/util/status",
        "@io_gorm_gorm//:gorm",
    ],
)
