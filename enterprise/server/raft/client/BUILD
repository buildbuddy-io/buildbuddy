load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "client",
    srcs = ["client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/client",
    deps = [
        "//enterprise/server/raft/config",
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/rbuilder",
        "//proto:raft_go_proto",
        "//proto:raft_service_go_proto",
        "//server/environment",
        "//server/metrics",
        "//server/util/grpc_client",
        "//server/util/lockmap",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/retry",
        "//server/util/status",
        "//server/util/tracing",
        "//server/util/uuid",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_lni_dragonboat_v4//:dragonboat",
        "@com_github_lni_dragonboat_v4//client",
        "@com_github_lni_dragonboat_v4//statemachine",
        "@com_github_prometheus_client_golang//prometheus",
        "@io_opentelemetry_go_otel//attribute",
        "@org_golang_google_genproto_googleapis_rpc//status",
        "@org_golang_google_grpc//status",
    ],
)

go_test(
    name = "client_test",
    srcs = ["client_test.go"],
    deps = [
        ":client",
        "//enterprise/server/raft/logger",
        "//enterprise/server/raft/rbuilder",
        "//enterprise/server/raft/testutil",
        "//proto:raft_go_proto",
        "//server/util/random",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_sync//errgroup",
    ],
)
