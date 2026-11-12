load("@io_bazel_rules_go//go:def.bzl", "go_test")
load("//server/util/bazel:defs.bzl", "bazel_versions_test")

package(default_visibility = ["//cli:__subpackages__"])

exports_files(
    [
        "testdata/1.binpb.zst",
        "testdata/2.binpb.zst",
    ],
    visibility = ["//cli:__subpackages__"],
)

bazel_versions_test(
    name = "compact_test",
    srcs = ["compact_test.go"],
    data = glob(["testdata/**"]),
    rule = go_test,
    deps = [
        "//cli/testutil/testcli",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
