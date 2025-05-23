load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "replica",
    srcs = ["replica.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/replica",
    deps = [
        "//enterprise/server/filestore",
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/events",
        "//enterprise/server/raft/keys",
        "//enterprise/server/util/pebble",
        "//proto:raft_go_proto",
        "//proto:storage_go_proto",
        "//server/metrics",
        "//server/util/canary",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/qps",
        "//server/util/rangemap",
        "//server/util/status",
        "@com_github_docker_go_units//:go-units",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_lni_dragonboat_v4//statemachine",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_genproto_googleapis_rpc//status",
        "@org_golang_google_grpc//status",
    ],
)

go_test(
    name = "replica_test",
    size = "small",
    srcs = ["replica_test.go"],
    deps = [
        ":replica",
        "//enterprise/server/filestore",
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/keys",
        "//enterprise/server/raft/rbuilder",
        "//enterprise/server/raft/testutil",
        "//enterprise/server/util/pebble",
        "//proto:raft_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//proto:storage_go_proto",
        "//server/interfaces",
        "//server/testutil/testdigest",
        "//server/testutil/testfs",
        "//server/util/disk",
        "//server/util/ioutil",
        "//server/util/proto",
        "//server/util/status",
        "//server/util/uuid",
        "@com_github_lni_dragonboat_v4//statemachine",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_rpc//status",
    ],
)
