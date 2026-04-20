load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "storemap",
    srcs = ["storemap.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/storemap",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/raft/constants",
        "//proto:raft_go_proto",
        "//server/interfaces",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
        "@com_github_hashicorp_serf//serf",
        "@com_github_jonboulle_clockwork//:clockwork",
    ],
)

go_test(
    name = "storemap_test",
    srcs = ["storemap_test.go"],
    embed = [":storemap"],
    deps = [
        "//enterprise/server/raft/constants",
        "//proto:raft_go_proto",
        "//server/interfaces",
        "//server/util/log",
        "//server/util/proto",
        "@com_github_hashicorp_serf//serf",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//require",
    ],
)
