load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "groupstatus",
    srcs = ["groupstatus.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/groupstatus",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:group_go_proto",
        "//server/interfaces",
        "//server/util/claims",
        "//server/util/status",
    ],
)

go_test(
    name = "groupstatus_test",
    srcs = ["groupstatus_test.go"],
    deps = [
        ":groupstatus",
        "//proto:group_go_proto",
        "//server/testutil/testauth",
        "//server/util/claims",
        "@com_github_stretchr_testify//require",
    ],
)
