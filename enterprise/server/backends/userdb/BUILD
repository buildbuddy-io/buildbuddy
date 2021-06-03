load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "userdb",
    srcs = ["userdb.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/userdb",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:api_key_go_proto",
        "//proto:group_go_proto",
        "//proto:telemetry_go_proto",
        "//server/environment",
        "//server/tables",
        "//server/util/capabilities",
        "//server/util/db",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/query_builder",
        "//server/util/random",
        "//server/util/status",
    ],
)
