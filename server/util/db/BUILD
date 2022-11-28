load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "db",
    srcs = ["db.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/db",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/metrics",
        "//server/tables",
        "//server/util/flagutil",
        "//server/util/log",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_aws_aws_sdk_go//aws/session",
        "@com_github_aws_aws_sdk_go//service/rds/rdsutils",
        "@com_github_go_sql_driver_mysql//:mysql",
        "@com_github_googlecloudplatform_cloudsql_proxy//proxy/dialers/mysql",
        "@com_github_mattn_go_sqlite3//:go-sqlite3",
        "@com_github_prometheus_client_golang//prometheus",
        "@io_gorm_driver_mysql//:mysql",
        "@io_gorm_driver_sqlite//:sqlite",
        "@io_gorm_gorm//:gorm",
        "@io_gorm_gorm//logger",
        "@io_opentelemetry_go_otel//attribute",
        "@io_opentelemetry_go_otel//codes",
        "@io_opentelemetry_go_otel_trace//:trace",
    ],
)

go_test(
    name = "db_test",
    srcs = ["db_test.go"],
    deps = [
        ":db",
        "@com_github_stretchr_testify//require",
    ],
)
