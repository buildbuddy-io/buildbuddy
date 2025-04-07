load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "proxy",
    srcs = ["proxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/proxy",
    visibility = ["//visibility:public"],
    deps = ["@org_golang_google_grpc//metadata"],
)

package(default_visibility = ["//enterprise:__subpackages__"])
