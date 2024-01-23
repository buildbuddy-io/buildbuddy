load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "featureflag_client",
    srcs = ["featureflag_client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/featureflag_client",
    deps = [
        "//enterprise/server/auth",
        "//proto:featureflag_go_proto",
        "//server/environment",
        "//server/featureflag/featureflag_cache",
        "//server/util/authutil",
        "//server/util/status",
    ],
)
