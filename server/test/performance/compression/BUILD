load("@io_bazel_rules_go//go:def.bzl", "go_test")

go_test(
    name = "compression_test",
    srcs = ["compression_test.go"],
    args = [
        "-test.bench=.",
    ],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/grpc_client",
        "@com_github_klauspost_compress//zstd",
        "@com_github_stretchr_testify//require",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//metadata",
    ],
)
