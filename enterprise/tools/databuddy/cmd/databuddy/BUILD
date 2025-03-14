load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "databuddy_lib",
    srcs = [
        "config.go",
        "databuddy.go",
        "datasource.go",
        "http.go",
        "plugins.go",
        "queries.go",
        "querydb.go",
        "vouchauth.go",
    ],
    embedsrcs = [
        "static/index.html",
        "static/styles.css",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/databuddy/cmd/databuddy",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/backends/configsecrets",
        "//enterprise/tools/databuddy/bundle",
        "//enterprise/tools/databuddy/proto:databuddy_go_proto",
        "//server/backends/blobstore",
        "//server/config",
        "//server/http/protolet",
        "//server/interfaces",
        "//server/util/db",
        "//server/util/flag",
        "//server/util/flagutil",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
        "//third_party/singleflight",
        "@com_github_clickhouse_clickhouse_go_v2//:clickhouse-go",
        "@in_gopkg_yaml_v2//:yaml_v2",
        "@io_gorm_driver_clickhouse//:clickhouse",
        "@io_gorm_gorm//:gorm",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_protobuf//encoding/protojson",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "databuddy",
    embed = [":databuddy_lib"],
    visibility = ["//visibility:public"],
)
