load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "workflow_test",
    srcs = ["workflow_test.go"],
    deps = [
        "//enterprise/server/test/integration/remote_execution/rbetest",
        "//enterprise/server/testutil/testgit",
        "//enterprise/server/webhooks/webhook_data",
        "//enterprise/server/workflow/service",
        "//proto:buildbuddy_service_go_proto",
        "//proto:context_go_proto",
        "//proto:invocation_go_proto",
        "//proto:user_id_go_proto",
        "//proto:workflow_go_proto",
        "//server/backends/repo_downloader",
        "//server/interfaces",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/testutil/app",
        "//server/testutil/testbazel",
        "//server/testutil/testenv",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
