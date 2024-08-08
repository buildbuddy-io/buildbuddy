load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "persistentworker",
    srcs = ["persistentworker.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/persistentworker",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/commandutil",
        "//enterprise/server/remote_execution/container",
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/remote_execution/workspace",
        "//proto:remote_execution_go_proto",
        "//proto:worker_go_proto",
        "//server/interfaces",
        "//server/util/background",
        "//server/util/lockingbuffer",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
        "@org_golang_google_protobuf//encoding/protojson",
        "@org_golang_google_protobuf//encoding/protowire",
    ],
)
