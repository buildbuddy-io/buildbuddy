load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["db.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/db",
    visibility = ["//visibility:public"],
    deps = [
        "//server/config:go_default_library",
        "//server/interfaces:go_default_library",
        "//server/metrics:go_default_library",
        "//server/tables:go_default_library",
        "//server/util/status:go_default_library",
        "@com_github_googlecloudplatform_cloudsql_proxy//proxy/dialers/mysql:go_default_library",
        "@com_github_jinzhu_gorm//:go_default_library",
        "@com_github_jinzhu_gorm//dialects/mysql:go_default_library",
        "@com_github_jinzhu_gorm//dialects/postgres:go_default_library",
        "@com_github_jinzhu_gorm//dialects/sqlite:go_default_library",
        "@com_github_prometheus_client_golang//prometheus:go_default_library",
    ],
)
