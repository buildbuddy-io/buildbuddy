load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "schema",
    srcs = ["schema.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema",
    visibility = ["//visibility:public"],
    deps = [
        "//server/build_event_protocol/invocation_format",
        "//server/tables",
        "//server/util/log",
        "//server/util/status",
        "@io_gorm_gorm//:gorm",
    ],
)

go_test(
    name = "schema_test",
    srcs = ["schema_test.go"],
    embed = [":schema"],
    deps = [
        "//server/tables",
        "@com_github_go_faker_faker_v4//:faker",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
