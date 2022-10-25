load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "secrets",
    srcs = ["secrets.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/secrets",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/util/keystore",
        "//proto:remote_execution_go_proto",
        "//proto:secrets_go_proto",
        "//server/environment",
        "//server/tables",
        "//server/util/hash",
        "//server/util/perms",
        "//server/util/query_builder",
        "//server/util/status",
    ],
)
