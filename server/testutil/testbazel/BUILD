load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testbazel",
    testonly = 1,
    srcs = ["testbazel.go"],
    data = ["//server/util/bazel:bazel-4.1_crossplatform"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/bazel",
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
    ],
)
