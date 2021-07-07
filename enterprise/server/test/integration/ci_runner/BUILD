load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "ci_runner_test",
    srcs = ["ci_runner_test.go"],
    data = [
        "//enterprise/server/cmd/ci_runner",
    ],
    shard_count = 4,
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//enterprise/server/testutil/testgit",
        "//proto:invocation_go_proto",
        "//server/testutil/buildbuddy",
        "//server/testutil/testbazel",
        "//server/testutil/testfs",
        "@com_github_google_uuid//:uuid",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
    ],
)
