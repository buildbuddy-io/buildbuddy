load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "replica",
    srcs = ["replica.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica",
    deps = [
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/filestore",
        "//enterprise/server/raft/keys",
        "//enterprise/server/raft/rbuilder",
        "//enterprise/server/raft/sender",
        "//enterprise/server/util/pebbleutil",
        "//proto:raft_go_proto",
        "//proto:raft_service_go_proto",
        "//server/interfaces",
        "//server/metrics",
        "//server/util/disk",
        "//server/util/log",
        "//server/util/qps",
        "//server/util/rangemap",
        "//server/util/status",
        "@com_github_cockroachdb_pebble//:pebble",
        "@com_github_lni_dragonboat_v3//statemachine",
        "@com_github_prometheus_client_golang//prometheus",
        "@go_googleapis//google/rpc:status_go_proto",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//proto",
    ],
)

go_test(
    name = "replica_test",
    size = "small",
    srcs = ["replica_test.go"],
    deps = [
        ":replica",
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/filestore",
        "//enterprise/server/raft/keys",
        "//enterprise/server/raft/rbuilder",
        "//enterprise/server/raft/sender",
        "//proto:raft_go_proto",
        "//proto:resource_go_proto",
        "//server/interfaces",
        "//server/testutil/testdigest",
        "//server/testutil/testfs",
        "//server/util/disk",
        "//server/util/ioutil",
        "//server/util/log",
        "//server/util/status",
        "@com_github_lni_dragonboat_v3//statemachine",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//proto",
    ],
)
