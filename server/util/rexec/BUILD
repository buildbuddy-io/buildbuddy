load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "rexec",
    srcs = ["rexec.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/rexec",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/proto",
        "//server/util/retry",
        "//server/util/status",
        "@org_golang_google_genproto//googleapis/longrunning",
        "@org_golang_google_grpc//status",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "rexec_test",
    srcs = ["rexec_test.go"],
    deps = [
        ":rexec",
        "//proto:remote_execution_go_proto",
        "//server/util/proto",
        "@com_github_google_go_cmp//cmp",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//testing/protocmp",
    ],
)
