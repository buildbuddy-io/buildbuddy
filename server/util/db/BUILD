load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "db",
    srcs = [
        "db.go",
        "db_cgo.go",
        "db_nocgo.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/db",
    visibility = ["//visibility:public"],
    deps = [
        "//aws_rds_certs:bundle",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/tables",
        "//server/util/flag",
        "//server/util/gormutil",
        "//server/util/log",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_aws_aws_sdk_go_v2//aws",
        "@com_github_aws_aws_sdk_go_v2_config//:config",
        "@com_github_aws_aws_sdk_go_v2_feature_rds_auth//:auth",
        "@com_github_go_sql_driver_mysql//:mysql",
        "@com_github_googlecloudplatform_cloudsql_proxy//proxy/dialers/mysql",
        "@com_github_jackc_pgerrcode//:pgerrcode",
        "@com_github_jackc_pgx_v5//pgconn",
        "@com_github_jackc_pgx_v5//stdlib",
        "@com_github_mattn_go_sqlite3//:go-sqlite3",
        "@com_github_prometheus_client_golang//prometheus",
        "@io_gorm_driver_mysql//:mysql",
        "@io_gorm_driver_postgres//:postgres",
        "@io_gorm_driver_sqlite//:sqlite",
        "@io_gorm_gorm//:gorm",
        "@io_gorm_gorm//logger",
        "@io_gorm_gorm//schema",
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
