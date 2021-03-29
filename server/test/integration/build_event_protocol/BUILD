load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "go_default_test",
    srcs = ["build_event_protocol_test.go"],
    deps = [
        "//server/testutil/bazel",
        "//server/testutil/buildbuddy",
        "@com_github_stretchr_testify//assert",
    ],
)

go_test(
    name = "build_event_protocol_test",
    srcs = ["build_event_protocol_test.go"],
    deps = [
        "//server/testutil/bazel",
        "//server/testutil/buildbuddy",
        "@com_github_stretchr_testify//assert",
    ],
)
