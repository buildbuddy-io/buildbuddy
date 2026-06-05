load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "rbuilder",
    srcs = ["rbuilder.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/rbuilder",
    deps = [
        "//enterprise/server/raft/constants",
        "//proto:raft_go_proto",
        "//server/util/proto",
        "//server/util/status",
        "@com_github_google_uuid//:uuid",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//encoding/prototext",
    ],
)

go_test(
    name = "rbuilder_test",
    srcs = ["rbuilder_test.go"],
    deps = [
        ":rbuilder",
        "//enterprise/server/raft/constants",
        "//enterprise/server/raft/keys",
        "//proto:raft_go_proto",
        "//server/util/status",
        "@com_github_stretchr_testify//require",
    ],
)
