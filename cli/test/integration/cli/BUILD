load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "cli_test",
    srcs = ["cli_test.go"],
    deps = [
        "//cli/log",
        "//cli/testutil/testcli",
        "//server/testutil/testbazel",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//require",
    ],
)
