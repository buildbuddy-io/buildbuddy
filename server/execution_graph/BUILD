load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "execution_graph",
    srcs = [
        "analyzer.go",
        "execution_graph.go",
        "service.go",
        "storage.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/execution_graph",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:execution_graph_analysis_go_proto",
        "//proto:execution_graph_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/compression",
        "//server/util/proto",
        "//server/util/status",
        "@org_golang_google_protobuf//encoding/protodelim",
    ],
)

go_test(
    name = "execution_graph_test",
    size = "small",
    srcs = [
        "analyzer_test.go",
        "storage_test.go",
    ],
    data = glob(["testdata/**"]),
    deps = [
        ":execution_graph",
        "//proto:execution_graph_analysis_go_proto",
        "//proto:execution_graph_go_proto",
        "//server/backends/blobstore/disk",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)
