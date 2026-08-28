load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "ext4writer",
    srcs = [
        "ext4reader.go",
        "ext4writer.go",
        "tree.go",
        "virtual.go",
        "xattr.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4writer",
    target_compatible_with = ["@platforms//os:linux"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/digest",
        "//server/util/status",
        "@org_golang_x_sync//errgroup",
        "@org_golang_x_sys//unix",
    ],
)

go_test(
    name = "ext4writer_test",
    srcs = ["ext4writer_test.go"],
    embed = [":ext4writer"],
    deps = [
        "//enterprise/server/util/ext4",
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/digest",
        "//server/testutil/testfs",
        "//server/util/log",
        "@com_github_stretchr_testify//require",
    ],
)
