load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "proxy_util",
    srcs = ["proxy_util.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/proxy_util",
    visibility = ["//visibility:public"],
    deps = ["@org_golang_google_grpc//metadata"],
)

package(default_visibility = ["//enterprise:__subpackages__"])
