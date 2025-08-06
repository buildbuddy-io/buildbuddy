load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "sender",
    srcs = ["sender.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/sender",
    deps = [
        "//enterprise/server/raft/client",
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/header",
        "//enterprise/server/raft/keys",
        "//enterprise/server/raft/rangecache",
        "//enterprise/server/raft/rbuilder",
        "//proto:raft_go_proto",
        "//proto:raft_service_go_proto",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/rangemap",
        "//server/util/retry",
        "//server/util/status",
        "//server/util/tracing",
        "@io_opentelemetry_go_otel//attribute",
        "@org_golang_google_grpc//status",
    ],
)

go_test(
    name = "sender_test",
    srcs = ["sender_test.go"],
    exec_properties = {
        "test.EstimatedComputeUnits": "8",
        "test.workload-isolation-type": "firecracker",
    },
    tags = ["block-network"],
    deps = [
        "//enterprise/server/raft/testutil",
        "//server/util/proto",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)
