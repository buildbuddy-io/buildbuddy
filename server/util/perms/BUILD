load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "perms",
    srcs = ["perms.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/perms",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:acl_go_proto",
        "//proto:user_id_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/authutil",
        "//server/util/log",
        "//server/util/query_builder",
        "//server/util/status",
    ],
)

go_test(
    name = "perms_test",
    srcs = ["perms_test.go"],
    deps = [
        ":perms",
        "//proto:acl_go_proto",
        "//proto:user_id_go_proto",
        "//server/interfaces",
        "//server/real_environment",
        "//server/testutil/testauth",
        "//server/util/status",
        "@com_github_stretchr_testify//require",
    ],
)
