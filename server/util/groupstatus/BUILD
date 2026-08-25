load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "groupstatus",
    srcs = ["groupstatus.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/groupstatus",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:group_go_proto",
        "//proto:remote_asset_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/environment",
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
        "//enterprise/server/experiments",
        "//proto:group_go_proto",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/claims",
        "@com_github_open_feature_go_sdk//openfeature",
        "@com_github_open_feature_go_sdk//openfeature/memprovider",
        "@com_github_stretchr_testify//require",
    ],
)
