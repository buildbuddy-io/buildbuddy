load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "artifactsigner",
    srcs = ["artifactsigner.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/artifactsigner",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/dirtools",
        "//proto:imagesigner_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/real_environment",
        "//server/remote_cache/digest",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])
