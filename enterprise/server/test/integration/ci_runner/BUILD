load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "ci_runner_test",
    srcs = ["ci_runner_test.go"],
    data = [
        "//enterprise/server/cmd/ci_runner",
        "//server/testutil/bazel:bazel-3.7.0_crossplatform",
    ],
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//proto:invocation_go_proto",
        "//server/testutil/bazel",
        "//server/testutil/buildbuddy",
        "@com_github_go_git_go_git_v5//:go-git",
        "@com_github_go_git_go_git_v5//config",
        "@com_github_go_git_go_git_v5//plumbing",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/tools/bazel",
    ],
)
