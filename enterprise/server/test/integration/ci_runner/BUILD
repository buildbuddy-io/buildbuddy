load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "ci_runner_test",
    size = "small",
    srcs = ["ci_runner_test.go"],
    data = [
        "//enterprise/server/cmd/ci_runner",
    ],
    # Run the ci_runner_test in the same environment that the CI runner uses in prod,
    # since we invoke the ci runner binary directly.
    exec_properties = {
        "container-image": "docker://gcr.io/flame-public/buildbuddy-ci-runner:v2.2.8",
    },
    shard_count = 9,
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//enterprise/server/testutil/testgit",
        "//proto:eventlog_go_proto",
        "//proto:invocation_go_proto",
        "//server/remote_cache/cachetools",
        "//server/testutil/app",
        "//server/testutil/buildbuddy",
        "//server/testutil/testbazel",
        "//server/testutil/testfs",
        "//server/testutil/testshell",
        "@com_github_google_uuid//:uuid",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
    ],
)
