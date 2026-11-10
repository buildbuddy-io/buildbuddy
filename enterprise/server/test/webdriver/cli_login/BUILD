load("//rules/webdriver:index.bzl", "go_web_test_suite")
load("//server/util/bazel:defs.bzl", "bazel_versions_test")

package(default_visibility = ["//enterprise:__subpackages__"])

bazel_versions_test(
    name = "cli_login_test",
    srcs = ["cli_login_test.go"],
    rule = go_web_test_suite,
    shard_count = 3,
    deps = [
        "//cli/testutil/testcli",
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//proto:buildbuddy_service_go_proto",
        "//proto:context_go_proto",
        "//proto:group_go_proto",
        "//proto:invocation_go_proto",
        "//server/testutil/webtester",
        "//server/util/grpc_client",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//metadata",
    ],
)
