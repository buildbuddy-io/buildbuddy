load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "proto",
    srcs = ["proto.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/proto",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/vtprotocodec",
        "@org_golang_google_grpc//encoding",
        "@org_golang_google_grpc//mem",
        "@org_golang_google_protobuf//proto",
    ],
)

go_test(
    name = "proto_test",
    srcs = ["proto_test.go"],
    deps = [
        ":proto",
        "//proto:cache_go_proto",
        "//proto:distributed_cache_go_proto",
        "//proto:storage_go_proto",
        "@com_github_go_faker_faker_v4//:faker",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//proto",
    ],
)

go_test(
    name = "proto_bench_test",
    srcs = ["proto_test.go"],
    args = [
        # can be narrow down to specific benchmark with this Bazel flag:
        #   --test_arg='-test.bench=BenchmarkMarshal/name=New/'
        "-test.bench=.",
    ],
    exec_properties = {
        "test.Pool": "bare",
        "test.use-self-hosted-executors": "true",
        "test.container-image": "none",
        "test.EstimatedComputeUnits": "16",
    },
    tags = [
        "manual",
        "no-cache",
    ],
    deps = [
        ":proto",
        "//proto:cache_go_proto",
        "//proto:distributed_cache_go_proto",
        "//proto:storage_go_proto",
        "@com_github_go_faker_faker_v4//:faker",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//proto",
    ],
)
