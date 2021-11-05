load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(
    default_testonly = 1,
    default_visibility = ["//server/util/fileresolver:__subpackages__"],
)

filegroup(
    name = "embedsrcs",
    srcs = [
        "embedded_dir/embedded_child.txt",
        "embedded_file.txt",
    ],
)

filegroup(
    name = "runfiles",
    srcs = ["runfile.txt"],
)

go_library(
    name = "bundle",
    srcs = ["bundle.go"],
    data = [":runfiles"],
    embedsrcs = [
        "BUILD",
        "bundle.go",
        "embedded_dir/embedded_child.txt",
        "embedded_file.txt",
        "runfile.txt",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/fileresolver/test_data",
    visibility = ["//visibility:public"],
)
