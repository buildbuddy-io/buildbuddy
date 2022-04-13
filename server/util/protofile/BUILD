load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "protofile",
    srcs = ["protofile.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/protofile",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/util/status",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//proto",
    ],
)
