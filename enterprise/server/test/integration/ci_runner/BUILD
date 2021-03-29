load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "go_default_test",
    srcs = ["ci_runner_test.go"],
    data = ["//enterprise/server/cmd/ci_runner"],
    # TODO: Make this automatic when we figure out a good way to include
    # a `git` binary in this test (since the action runner needs it).
    # Also would need to support a BAZEL_BINARY env variable or flag
    # to tell it to use a fixed version of Bazel that we include via
    # data dep.
    tags = ["manual"],
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//proto:invocation_go_proto",
        "//server/testutil/bazel:go_default_library",
        "//server/testutil/buildbuddy:go_default_library",
        "@com_github_stretchr_testify//assert:go_default_library",
        "@com_github_stretchr_testify//require:go_default_library",
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
    ],
)

go_test(
    name = "ci_runner_test",
    srcs = ["ci_runner_test.go"],
    deps = [
        "//proto:invocation_go_proto",
        "//server/testutil/bazel",
        "//server/testutil/buildbuddy",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
    ],
)
