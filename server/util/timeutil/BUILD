load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "timeutil",
    srcs = ["timeutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/timeutil",
    visibility = ["//visibility:public"],
    deps = [
        "@org_golang_google_protobuf//types/known/durationpb",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)
