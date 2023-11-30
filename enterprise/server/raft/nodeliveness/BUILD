load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "nodeliveness",
    srcs = ["nodeliveness.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/nodeliveness",
    deps = [
        "//enterprise/server/raft/client",
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/keys",
        "//enterprise/server/raft/rbuilder",
        "//proto:raft_go_proto",
        "//server/util/canary",
        "//server/util/log",
        "//server/util/status",
        "@com_github_hashicorp_serf//serf",
        "@org_golang_google_protobuf//proto",
    ],
)

go_test(
    name = "nodeliveness_test",
    size = "small",
    srcs = ["nodeliveness_test.go"],
    deps = [
        ":nodeliveness",
        "//enterprise/server/raft/constants",
        "//proto:raft_go_proto",
        "//server/util/status",
        "@com_github_lni_dragonboat_v4//:dragonboat",
        "@com_github_lni_dragonboat_v4//client",
        "@com_github_lni_dragonboat_v4//statemachine",
        "@com_github_lni_goutils//random",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_rpc//status",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//proto",
    ],
)
