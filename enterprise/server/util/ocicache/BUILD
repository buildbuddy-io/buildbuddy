load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "ocicache",
    srcs = ["ocicache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ocicache",
    deps = [
        "//proto:ociregistry_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/metrics",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/flag",
        "//server/util/ioutil",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
        "@com_github_google_go_containerregistry//pkg/name",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_protobuf//types/known/anypb",
    ],
)
