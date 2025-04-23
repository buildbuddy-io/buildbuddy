load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testbazel",
    testonly = 1,
    srcs = ["testbazel.go"],
    data = [
        "//server/util/bazel:bazel-8.1.1_crossplatform",
        "//server/util/bazel:bazel-8.1.1_extract_installation",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel",
    visibility = ["//visibility:public"],
    x_defs = {
        "Version": "8.1.1",
        "bazelRlocationpath": "$(rlocationpath //server/util/bazel:bazel-8.1.1_crossplatform)",
        "outdirRlocationpath": "$(rlocationpath //server/util/bazel:bazel-8.1.1_extract_installation)",
    },
    deps = [
        "//server/testutil/testfs",
        "//server/util/bazel",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles:go_default_library",
    ],
)
