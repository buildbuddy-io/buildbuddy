load("@io_bazel_rules_go//go:def.bzl", "go_test")
load("//server/util/bazel:defs.bzl", "bazel_versions_test")

package(default_visibility = ["//enterprise:__subpackages__"])

bazel_versions_test(
    name = "stream_run_logs_test",
    srcs = ["stream_run_logs_test.go"],
    exec_properties = {
        # TODO: remove network dependency.
        "test.dockerNetwork": "bridge",
    },
    rule = go_test,
    deps = [
        "//cli/stream_run_logs",
        "//cli/testutil/testcli",
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//proto:api_key_go_proto",
        "//proto:buildbuddy_service_go_proto",
        "//proto:capability_go_proto",
        "//proto:eventlog_go_proto",
        "//proto:invocation_go_proto",
        "//proto:invocation_status_go_proto",
        "//server/tables",
        "//server/testutil/app",
        "//server/testutil/testfs",
        "//server/util/grpc_client",
        "//server/util/perms",
        "@com_github_google_uuid//:uuid",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//:grpc",
    ],
)
