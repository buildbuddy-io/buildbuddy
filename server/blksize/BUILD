load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "blksize_test",
    srcs = ["blksize_test.go"],
    deps = [
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//require",
    ],
)
