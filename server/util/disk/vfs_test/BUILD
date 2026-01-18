load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "vfs_test_test",
    srcs = ["disk_vfs_test.go"],
    deps = [
        "//server/testutil/testfs",
        "//server/util/disk",
        "//server/util/fsync",
        "@com_github_stretchr_testify//require",
    ],
)
