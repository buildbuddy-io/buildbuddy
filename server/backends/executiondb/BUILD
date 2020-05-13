load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["executiondb.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/executiondb",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/tables:go_default_library",
        "//server/util/db:go_default_library",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@com_github_jinzhu_gorm//:go_default_library",
        "@go_googleapis//google/longrunning:longrunning_go_proto",
    ],
)
