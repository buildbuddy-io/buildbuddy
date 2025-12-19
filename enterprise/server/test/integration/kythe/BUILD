load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "kythe_test",
    size = "large",
    srcs = ["kythe_test.go"],
    data = [
        "//enterprise/server/cmd/ci_runner",
    ],
    exec_properties = {
        "test.EstimatedComputeUnits": "3",
    },
    x_defs = {
        "ciRunnerRunfilePath": "$(rlocationpath //enterprise/server/cmd/ci_runner)",
    },
    deps = [
        "//server/testutil/buildbuddy",
        "//server/testutil/testbazel",
        "//server/testutil/testfs",
        "//server/testutil/testgit",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)
