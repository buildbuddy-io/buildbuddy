load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "driver",
    srcs = ["driver.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/driver",
    deps = [
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/events",
        "//enterprise/server/raft/store",
        "//proto:raft_go_proto",
        "//proto:raft_service_go_proto",
        "//server/interfaces",
        "//server/util/log",
        "//server/util/status",
        "//server/util/statusz",
        "@com_github_hashicorp_serf//serf",
        "@org_golang_google_protobuf//proto",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "driver_test",
    srcs = ["driver_test.go"],
    deps = [
        ":driver",
        "//proto:raft_go_proto",
        "@com_github_hashicorp_serf//serf",
        "@com_github_stretchr_testify//require",
    ],
)
