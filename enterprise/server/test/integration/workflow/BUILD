load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "workflow_test",
    srcs = ["workflow_test.go"],
    shard_count = 6,
    tags = ["block-network"],
    deps = [
        "//enterprise/server/githubapp",
        "//enterprise/server/invocation_search_service",
        "//enterprise/server/test/integration/remote_execution/rbetest",
        "//enterprise/server/workflow/service",
        "//proto:buildbuddy_service_go_proto",
        "//proto:context_go_proto",
        "//proto:eventlog_go_proto",
        "//proto:invocation_go_proto",
        "//proto:invocation_status_go_proto",
        "//proto:user_id_go_proto",
        "//proto:workflow_go_proto",
        "//server/backends/github",
        "//server/backends/repo_downloader",
        "//server/endpoint_urls/build_buddy_url",
        "//server/interfaces",
        "//server/tables",
        "//server/testutil/testbazel",
        "//server/testutil/testenv",
        "//server/testutil/testgit",
        "//server/testutil/testhttp",
        "//server/util/perms",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//codes",
    ],
)
