load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

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

go_test(
    name = "secrets_test",
    srcs = ["secrets_test.go"],
    deps = [
        ":secrets",
        "//enterprise/server/backends/userdb",
        "//proto:secrets_go_proto",
        "//server/tables",
        "//server/testutil/testenv",
        "@com_github_stretchr_testify//require",
    ],
)
