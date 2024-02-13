load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "analysis",
    srcs = ["analysis.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/analysis",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/flag",
        "//server/util/status",
        "//proto:analysis_go_proto",
        "@org_golang_google_protobuf//encoding/protojson",
    ],
)
