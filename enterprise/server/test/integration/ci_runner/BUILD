load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "ci_runner_test",
    srcs = ["ci_runner_test.go"],
    data = [
        "//enterprise/server/cmd/ci_runner",
    ],
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//proto:invocation_go_proto",
        "//server/testutil/buildbuddy",
        "//server/testutil/testbazel",
        "@com_github_go_git_go_git_v5//:go-git",
        "@com_github_go_git_go_git_v5//plumbing/object",
        "@com_github_google_uuid//:uuid",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
    ],
)
