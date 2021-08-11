load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "runner",
    srcs = ["runner.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/runner",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/operation",
        "//proto:remote_execution_go_proto",
        "//proto:runner_go_proto",
        "//server/environment",
        "//server/remote_cache/cachetools",
        "//server/tables",
        "//server/util/db",
        "//server/util/log",
        "//server/util/prefix",
        "//server/util/query_builder",
        "//server/util/status",
        "@com_github_google_uuid//:uuid",
        "@go_googleapis//google/longrunning:longrunning_go_proto",
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
    ],
)
