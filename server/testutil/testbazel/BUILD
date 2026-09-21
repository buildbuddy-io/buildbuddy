load("@io_bazel_rules_go//go:def.bzl", "go_library")
load("//server/util/bazel:defs.bzl", "bazel_binary_targets")

bazel_binary_targets(
    versions = ["9.2.0"],
    visibility = ["//visibility:public"],
)

go_library(
    name = "testbazel",
    testonly = 1,
    srcs = ["testbazel.go"],
    data = [
        ":bazel-9.2.0_crossplatform",
        ":bazel-9.2.0_extract_installation",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel",
    visibility = ["//visibility:public"],
    x_defs = {
        "Version": "9.2.0",
        "bazelRlocationpath": "$(rlocationpath :bazel-9.2.0_crossplatform)",
        "outdirRlocationpath": "$(rlocationpath :bazel-9.2.0_extract_installation)",
    },
    deps = [
        "//server/testutil/testfs",
        "//server/util/bazel",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)
