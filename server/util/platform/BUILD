load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "platform",
    srcs = ["platform.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/platform",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:firecracker_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//server/util/log",
        "//server/util/status",
        "@com_github_docker_go_units//:go-units",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_protobuf//encoding/protojson",
    ],
)

go_test(
    name = "platform_test",
    size = "small",
    srcs = ["platform_test.go"],
    embed = [":platform"],
    deps = [
        "//proto:firecracker_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:scheduler_go_proto",
        "//server/util/status",
        "@com_github_google_go_cmp//cmp",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//testing/protocmp",
    ],
)
