load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "eventlog",
    srcs = ["eventlog.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/eventlog",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:eventlog_go_proto",
        "//proto:invocation_status_go_proto",
        "//server/backends/chunkstore",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/util/db",
        "//server/util/keyval",
        "//server/util/proto",
        "//server/util/status",
        "//server/util/terminal",
    ],
)

go_test(
    name = "eventlog_test",
    srcs = ["eventlog_test.go"],
    deps = [
        ":eventlog",
        "//enterprise/server/experiments",
        "//proto:eventlog_go_proto",
        "//proto:invocation_status_go_proto",
        "//server/backends/chunkstore",
        "//server/backends/memory_kvstore",
        "//server/tables",
        "//server/testutil/mockinvocationdb",
        "//server/testutil/mockstore",
        "//server/testutil/testenv",
        "//server/util/status",
        "//server/util/terminal",
        "//server/util/terminal/testdata",
        "@com_github_open_feature_go_sdk//openfeature",
        "@com_github_open_feature_go_sdk//openfeature/memprovider",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
