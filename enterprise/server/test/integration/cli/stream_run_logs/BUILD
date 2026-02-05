load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "stream_run_logs_test",
    srcs = ["stream_run_logs_test.go"],
    exec_properties = {
        # TODO: remove network dependency.
        "test.dockerNetwork": "bridge",
    },
    deps = [
        "//cli/stream_run_logs",
        "//cli/testutil/testcli",
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//proto:api_key_go_proto",
        "//proto:capability_go_proto",
        "//proto:eventlog_go_proto",
        "//proto:invocation_go_proto",
        "//proto:invocation_status_go_proto",
        "//server/tables",
        "//server/testutil/app",
        "//server/testutil/testbazel",
        "//server/testutil/testfs",
        "//server/util/perms",
        "@com_github_google_uuid//:uuid",
        "@com_github_stretchr_testify//require",
    ],
)
