load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")

go_library(
    name = "bazelrbe_lib",
    srcs = ["bazelrbe.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/probers/bazelrbe",
    visibility = ["//visibility:private"],
    deps = [
        "//server/util/log",
        "//server/util/status",
    ],
)

go_binary(
    name = "bazelrbe",
    embed = [":bazelrbe_lib"],
    visibility = ["//visibility:public"],
)

go_test(
    name = "bazelrbe_test",
    size = "medium",
    srcs = ["bazelrbe_test.go"],
    data = [":bazelrbe"],
    x_defs = {
        "proberBinaryRunfilePath": "$(rlocationpath :bazelrbe)",
    },
    deps = [
        "//enterprise/server/test/integration/remote_execution/rbetest",
        "//server/testutil/testbazel",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)
