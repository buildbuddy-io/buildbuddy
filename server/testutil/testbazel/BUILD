load("@io_bazel_rules_go//go:def.bzl", "go_library")
load("//server/util/bazel:versions.bzl", "TESTBAZEL_VERSION")

go_library(
    name = "testbazel",
    testonly = 1,
    srcs = ["testbazel.go"],
    data = [
        "//server/util/bazel:bazel-%s_crossplatform" % TESTBAZEL_VERSION,
        "//server/util/bazel:bazel-%s_extract_installation" % TESTBAZEL_VERSION,
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testbazel",
    visibility = ["//visibility:public"],
    x_defs = {
        "Version": TESTBAZEL_VERSION,
        "bazelRlocationpath": "$(rlocationpath //server/util/bazel:bazel-%s_crossplatform)" % TESTBAZEL_VERSION,
        "outdirRlocationpath": "$(rlocationpath //server/util/bazel:bazel-%s_extract_installation)" % TESTBAZEL_VERSION,
    },
    deps = [
        "//server/testutil/testfs",
        "//server/util/bazel",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)
