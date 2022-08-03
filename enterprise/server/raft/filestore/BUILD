load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "filestore",
    srcs = ["filestore.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/keys",
        "//proto:raft_go_proto",
        "//server/util/disk",
        "//server/util/status",
        "@com_github_cockroachdb_pebble//:pebble",
    ],
)

go_test(
    name = "filestore_test",
    size = "small",
    srcs = ["filestore_test.go"],
    deps = [
        ":filestore",
        "//proto:raft_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/testutil/testfs",
        "//server/util/status",
        "@com_github_cockroachdb_pebble//:pebble",
        "@com_github_stretchr_testify//require",
    ],
)
