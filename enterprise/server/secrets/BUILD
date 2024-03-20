load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "secrets",
    srcs = ["secrets.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/secrets",
    deps = [
        "//enterprise/server/util/keystore",
        "//proto:remote_execution_go_proto",
        "//proto:secrets_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/real_environment",
        "//server/tables",
        "//server/util/authutil",
        "//server/util/db",
        "//server/util/hash",
        "//server/util/perms",
        "//server/util/query_builder",
        "//server/util/status",
    ],
)

go_test(
    name = "secrets_test",
    size = "small",
    srcs = ["secrets_test.go"],
    deps = [
        ":secrets",
        "//enterprise/server/backends/kms",
        "//enterprise/server/testutil/enterprise_testauth",
        "//enterprise/server/testutil/enterprise_testenv",
        "//enterprise/server/util/keystore",
        "//proto:secrets_go_proto",
        "//server/tables",
        "//server/testutil/testfs",
        "//server/util/authutil",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
