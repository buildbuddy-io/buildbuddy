load("@io_bazel_rules_go//go:def.bzl", "go_test")

# Linearizability tests for the metadata server. Tagged "manual" so
# they only run when explicitly targeted.
#
# Default run (3 min per test, 15 min timeout):
#   bb test //enterprise/server/raft/store/linearizability:linearizability_test
#
# Longer run (1 hour):
#   bb test //enterprise/server/raft/store/linearizability:linearizability_test \
#     --test_timeout=4200 --test_env=TEST_DURATION=1h
go_test(
    name = "linearizability_test",
    timeout = "long",
    srcs = ["linearizability_test.go"],
    env = {
        "TEST_DURATION": "3m",
    },
    exec_properties = {
        "test.EstimatedComputeUnits": "10",
        "test.workload-isolation-type": "firecracker",
    },
    tags = [
        "block-network",
        "manual",
    ],
    deps = [
        "//enterprise/server/filestore",
        "//enterprise/server/raft/rbuilder",
        "//enterprise/server/raft/testutil",
        "//proto:raft_go_proto",
        "//proto:storage_go_proto",
        "//server/testutil/testdigest",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/testing/flags",
        "@com_github_anishathalye_porcupine//:porcupine",
        "@com_github_stretchr_testify//require",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
