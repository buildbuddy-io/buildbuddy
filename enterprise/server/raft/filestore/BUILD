load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "filestore",
    srcs = ["filestore.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/filestore",
    deps = [
        "//proto:raft_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/interfaces",
        "//server/util/disk",
        "//server/util/log",
        "//server/util/status",
    ],
)

go_test(
    name = "filestore_test",
    srcs = ["filestore_test.go"],
    deps = [
        ":filestore",
        "//proto:raft_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/testutil/testdigest",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
