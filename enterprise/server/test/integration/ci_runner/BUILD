load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "ci_runner_test",
    size = "small",
    srcs = ["ci_runner_test.go"],
    data = [
        "//enterprise/server/cmd/ci_runner",
    ],
    shard_count = 31,
    tags = ["block-network"],
    x_defs = {
        "ciRunnerRunfilePath": "$(rlocationpath //enterprise/server/cmd/ci_runner)",
    },
    deps = [
        "//enterprise/server/workflow/config",
        "//proto:build_event_stream_go_proto",
        "//proto:command_line_go_proto",
        "//proto:eventlog_go_proto",
        "//proto:invocation_go_proto",
        "//proto:invocation_status_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:remote_execution_log_go_proto",
        "//proto:runner_go_proto",
        "//server/remote_cache/cachetools",
        "//server/testutil/app",
        "//server/testutil/buildbuddy",
        "//server/testutil/testbazel",
        "//server/testutil/testfs",
        "//server/testutil/testgit",
        "//server/testutil/testshell",
        "@com_github_google_uuid//:uuid",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@in_gopkg_yaml_v2//:yaml_v2",
        "@io_bazel_rules_go//go/runfiles",
        "@org_golang_google_protobuf//encoding/protodelim",
    ],
)
