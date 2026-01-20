load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "cli_plugins_test",
    srcs = ["upload_run_logs_test.go"],
    data = ["//cli/plugins/upload_run_logs"],
    deps = [
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//proto:api_key_go_proto",
        "//proto:capability_go_proto",
        "//proto:eventlog_go_proto",
        "//proto:invocation_go_proto",
        "//proto:invocation_status_go_proto",
        "//server/tables",
        "//server/testutil/testfs",
        "//server/util/perms",
        "@com_github_google_uuid//:uuid",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)
