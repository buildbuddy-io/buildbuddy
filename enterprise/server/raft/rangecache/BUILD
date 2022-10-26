load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "rangecache",
    srcs = ["rangecache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rangecache",
    deps = [
        "//enterprise/server/raft/constants",
        "//proto:raft_go_proto",
        "//server/util/log",
        "//server/util/rangemap",
        "//server/util/status",
        "@com_github_hashicorp_serf//serf",
        "@org_golang_google_protobuf//proto",
    ],
)

go_test(
    name = "rangecache_test",
    size = "small",
    srcs = ["rangecache_test.go"],
    deps = [
        ":rangecache",
        "//enterprise/server/raft/constants",
        "//proto:raft_go_proto",
        "//server/util/log",
        "@com_github_hashicorp_serf//serf",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//proto",
    ],
)
