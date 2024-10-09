load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testspanner",
    testonly = 1,
    srcs = ["testspanner.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testspanner",
    visibility = ["//visibility:public"],
    deps = [
        "//server/testutil/dockerutil",
        "//server/testutil/testport",
        "//server/util/log",
        "@com_github_googleapis_go_sql_spanner//:go-sql-spanner",
        "@com_github_jackc_pgx_v5//:pgx",
        "@com_github_stretchr_testify//require",
        "@com_google_cloud_go_spanner//admin/database/apiv1",
        "@com_google_cloud_go_spanner//admin/database/apiv1/databasepb",
        "@com_google_cloud_go_spanner//admin/instance/apiv1",
        "@com_google_cloud_go_spanner//admin/instance/apiv1/instancepb",
        "@org_golang_google_api//option",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//credentials/insecure",
    ],
)
