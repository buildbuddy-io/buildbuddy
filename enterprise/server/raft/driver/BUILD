load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "driver",
    srcs = ["driver.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/driver",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/raft/config",
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/header",
        "//enterprise/server/raft/replica",
        "//enterprise/server/raft/storemap",
        "//proto:raft_go_proto",
        "//server/interfaces",
        "//server/util/alert",
        "//server/util/log",
        "//server/util/status",
        "@com_github_jonboulle_clockwork//:clockwork",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "driver_test",
    srcs = ["driver_test.go"],
    embed = [":driver"],
    deps = [
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/storemap",
        "//proto:raft_go_proto",
        "//server/util/log",
        "//server/util/proto",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//require",
    ],
)
