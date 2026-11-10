load("@io_bazel_rules_go//go:def.bzl", "go_test")
load("//server/util/bazel:defs.bzl", "bazel_versions_test")

package(default_visibility = ["//enterprise:__subpackages__"])

bazel_versions_test(
    name = "invocation_webhook_test",
    srcs = ["invocation_webhook_test.go"],
    rule = go_test,
    deps = [
        "//enterprise/server/backends/userdb",
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//proto:invocation_go_proto",
        "//server/real_environment",
        "//server/tables",
        "//server/testutil/testbazel",
        "//server/testutil/testfs",
        "//server/testutil/testhealthcheck",
        "//server/testutil/testhttp",
        "//server/util/db",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//encoding/protojson",
    ],
)
