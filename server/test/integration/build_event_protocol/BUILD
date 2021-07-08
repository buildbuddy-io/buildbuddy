load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "build_event_protocol_test",
    srcs = ["build_event_protocol_test.go"],
    tags = ["manual"],
    deps = [
        "//server/testutil/buildbuddy",
        "//server/testutil/testbazel",
        "@com_github_stretchr_testify//assert",
    ],
)
