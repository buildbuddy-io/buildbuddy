load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "nodeliveness",
    srcs = ["nodeliveness.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/nodeliveness",
    deps = [
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/keys",
        "//enterprise/server/raft/rbuilder",
        "//enterprise/server/raft/sender",
        "//proto:raft_go_proto",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_hashicorp_serf//serf",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "nodeliveness_test",
    size = "small",
    srcs = ["nodeliveness_test.go"],
    deps = [
        ":nodeliveness",
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/sender",
        "//proto:raft_go_proto",
        "//server/util/status",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_rpc//status",
        "@org_golang_google_grpc//status",
    ],
)
