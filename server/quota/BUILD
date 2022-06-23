load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "quota",
    srcs = ["quota_manager.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/quota",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/util/alert",
        "//server/util/db",
        "//server/util/log",
        "//server/util/status",
        "@com_github_throttled_throttled_v2//:throttled",
        "@com_github_throttled_throttled_v2//store/goredisstore.v8:goredisstore_v8",
        "@org_golang_google_grpc//metadata",
    ],
)

go_test(
    name = "quota_test",
    srcs = ["quota_manager_test.go"],
    embed = [":quota"],
    deps = [
        "//server/environment",
        "//server/tables",
        "//server/testutil/testenv",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
