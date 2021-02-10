load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "go_default_test",
    srcs = ["remote_cache_test.go"],
    # TODO: Make this test automatic instead of manual. We can either use a
    # cross-platform build of the dependent bazel binary, or upgrade to
    # Bazel 4.0.0 and set "target_compatible_with" on this test, so that it
    # only gets run on Linux when running `bazel test //...`
    tags = ["manual"],
    deps = [
        "//server/testutil/bazel:go_default_library",
        "//server/testutil/buildbuddy:go_default_library",
        "@com_github_stretchr_testify//assert:go_default_library",
    ],
)
