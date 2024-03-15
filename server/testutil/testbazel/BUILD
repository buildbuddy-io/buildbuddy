load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testbazel",
    testonly = 1,
    srcs = ["testbazel.go"],
    data = [
        "//server/util/bazel:bazel-5.3.2_crossplatform",
        "//server/util/bazel:bazel-5.3.2_extract_installation",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel",
    visibility = ["//visibility:public"],
    x_defs = {
        "Version": "5.3.2",
        "BazelBinaryPath": "$(rlocationpath //server/util/bazel:bazel-5.3.2_crossplatform)",
        "installBasePath": "$(rlocationpath //server/util/bazel:bazel-5.3.2_extract_installation)",
    },
    deps = [
        "//server/testutil/testfs",
        "//server/util/bazel",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles:go_default_library",
    ],
)
