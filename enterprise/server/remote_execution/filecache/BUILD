load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "filecache",
    srcs = ["filecache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/util/disk",
        "//server/util/fastcopy",
        "//server/util/log",
        "//server/util/lru",
        "//server/util/status",
        "@com_github_google_uuid//:uuid",
    ],
)

go_test(
    name = "filecache_test",
    srcs = ["filecache_test.go"],
    deps = [
        ":filecache",
        "//proto:remote_execution_go_proto",
        "//server/testutil/testfs",
        "//server/util/hash",
        "@com_github_stretchr_testify//assert",
    ],
)
