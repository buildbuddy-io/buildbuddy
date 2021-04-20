load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "db",
    srcs = ["db.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/db",
    visibility = ["//visibility:public"],
    deps = [
        "//server/config",
        "//server/interfaces",
        "//server/metrics",
        "//server/tables",
        "//server/util/log",
        "//server/util/status",
        "@com_github_googlecloudplatform_cloudsql_proxy//proxy/dialers/mysql",
        "@com_github_prometheus_client_golang//prometheus",
        "@io_gorm_driver_mysql//:mysql",
        "@io_gorm_driver_postgres//:postgres",
        "@io_gorm_driver_sqlite//:sqlite",
        "@io_gorm_gorm//:gorm",
        "@io_gorm_gorm//logger",
    ],
)
